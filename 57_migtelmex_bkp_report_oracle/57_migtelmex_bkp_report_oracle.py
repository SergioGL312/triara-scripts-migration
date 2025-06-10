import oci
import csv
import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import threading
from contextlib import contextmanager


@dataclass
class BackupInfo:
    """Estructura de datos para información de backup."""
    compartment_name: str
    database_name: str
    backup_name: str
    size_gb: str
    backup_type: str
    state: str
    time_started: datetime
    time_ended: Optional[datetime]
    ocid: str

    def to_dict(self) -> Dict[str, str]:
        """Convierte a diccionario para CSV."""
        return {
            'COMPARTMENT-NAME': self.compartment_name,
            'DATABASE-NAME': self.database_name,
            'NAME-BACKUP': self.backup_name,
            'SIZE_DB(GB)': self.size_gb,
            'TYPE': self.backup_type,
            'STATE': self.state,
            'TIME-STARTED': self.time_started,
            'TIME-ENDED': self.time_ended or 'N/A',
            'OCID': self.ocid,
        }


class RateLimiter:
    """Manejador de rate limiting con circuit breaker."""
    
    def __init__(self, min_interval: float = 0.5, max_retries: int = 3, backoff_factor: float = 2):
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.lock = threading.Lock()
        self.last_request_time = 0
        self.consecutive_failures = 0
        self.circuit_open_until = 0

    @contextmanager
    def rate_limit(self):
        """Context manager para rate limiting."""
        with self.lock:
            if time.time() < self.circuit_open_until:
                raise Exception("Circuit breaker is open")
            
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_interval:
                time.sleep(self.min_interval - time_since_last)
            
            self.last_request_time = time.time()
            yield

    def execute_with_retry(self, func, *args, **kwargs):
        """Ejecuta función con retry logic y circuit breaker."""
        for attempt in range(self.max_retries):
            try:
                with self.rate_limit():
                    result = func(*args, **kwargs)
                    self.consecutive_failures = 0
                    return result
                    
            except oci.exceptions.ServiceError as e:
                if e.status == 429:
                    wait_time = self.backoff_factor ** attempt
                    logging.warning(f"Rate limit reached. Waiting {wait_time}s (attempt {attempt + 1})")
                    time.sleep(wait_time)
                    if attempt == self.max_retries - 1:
                        self._handle_failure()
                        raise
                else:
                    self._handle_failure()
                    raise
            except Exception as e:
                self._handle_failure()
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(1)

    def _handle_failure(self):
        """Maneja fallos consecutivos y activa circuit breaker si es necesario."""
        self.consecutive_failures += 1
        if self.consecutive_failures >= 5:
            self.circuit_open_until = time.time() + 60
            logging.error("Circuit breaker opened due to consecutive failures")


class OracleBackupOptimized:
    """Clase optimizada para recolección de backups de Oracle Cloud."""
    
    def __init__(self, config_file: str = "~/.oci/config", profile: str = "trmxmigtelmex"):
        self.config = oci.config.from_file(config_file, profile)
        self.identity_client = oci.identity.IdentityClient(self.config)
        self.database_client = oci.database.DatabaseClient(self.config)
        self.tenancy_id = self.config["tenancy"]
        
        self.rate_limiter = RateLimiter()
        
        self._setup_date_range()
        
        self._setup_directories()
        
        self._setup_logging()
        
        self.csv_filename = os.path.join(
            self.reports_dir, 
            f"backups-db-oracle-{self.yesterday_start.date()}.csv"
        )

    def _setup_date_range(self):
        """Configura el rango de fechas para la búsqueda."""
        self.now_utc = datetime.now(timezone.utc)
        self.yesterday_start = (
            self.now_utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        )
        self.yesterday_end = self.yesterday_start + timedelta(days=1)

    def _setup_directories(self):
        """Configura los directorios necesarios."""
        base_dir = os.path.join(os.getcwd(), "backup-report")
        self.reports_dir = os.path.join(base_dir, "reports", "telmex")
        self.logs_dir = os.path.join(base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

    def _setup_logging(self):
        """Configura el sistema de logging."""
        log_filename = os.path.join(self.logs_dir, "migtelmex-backups-db-oracle.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_all_compartments(self) -> List[oci.identity.models.Compartment]:
        """Obtiene todos los compartimentos activos del tenancy."""
        try:
            compartments = self.rate_limiter.execute_with_retry(
                self.identity_client.list_compartments,
                compartment_id=self.tenancy_id,
                compartment_id_in_subtree=True,
                access_level="ANY"
            ).data
            
            active_compartments = [c for c in compartments if c.lifecycle_state == "ACTIVE"]
            self.logger.info(f"Found {len(active_compartments)} active compartments")
            return active_compartments
            
        except Exception as e:
            self.logger.error(f"Error fetching compartments: {str(e)}")
            return []

    def get_db_homes_batch(self, compartment_ids: List[str]) -> Dict[str, List]:
        """Obtiene DB Homes para múltiples compartimentos en lote."""
        db_homes_by_compartment = {}
        
        for compartment_id in compartment_ids:
            try:
                db_homes = self.rate_limiter.execute_with_retry(
                    self.database_client.list_db_homes,
                    compartment_id=compartment_id
                ).data
                db_homes_by_compartment[compartment_id] = db_homes
                
            except Exception as e:
                self.logger.error(f"Error fetching DB Homes for compartment {compartment_id}: {str(e)}")
                db_homes_by_compartment[compartment_id] = []
        
        return db_homes_by_compartment

    def process_database_backups(self, database, compartment_name: str) -> List[BackupInfo]:
        """Procesa los backups de una base de datos específica."""
        backup_infos = []
        
        try:
            backups = self.rate_limiter.execute_with_retry(
                self.database_client.list_backups,
                database_id=database.id
            ).data
            
            # Filtrar backups del día anterior
            filtered_backups = [
                b for b in backups 
                if b.time_ended and self.yesterday_start <= b.time_ended < self.yesterday_end
            ]
            
            for backup in filtered_backups:
                backup_info = BackupInfo(
                    compartment_name=compartment_name,
                    database_name=database.db_name,
                    backup_name=backup.display_name,
                    size_gb=str(getattr(backup, 'database_size_in_gbs', 'N/A')),
                    backup_type=getattr(backup, 'type', 'N/A'),
                    state=backup.lifecycle_state,
                    time_started=backup.time_started,
                    time_ended=getattr(backup, 'time_ended', None),
                    ocid=backup.id
                )
                backup_infos.append(backup_info)
                
        except Exception as e:
            self.logger.error(f"Error processing backups for database {database.db_name}: {str(e)}")
        
        return backup_infos

    def process_compartment(self, compartment) -> List[BackupInfo]:
        """Procesa un compartimento completo."""
        self.logger.info(f"Processing compartment: {compartment.name}")
        backup_infos = []
        
        try:
            db_homes = self.rate_limiter.execute_with_retry(
                self.database_client.list_db_homes,
                compartment_id=compartment.id
            ).data
            
            if not db_homes:
                self.logger.info(f"No DB Homes found in compartment: {compartment.name}")
                return backup_infos
            
            for db_home in db_homes:
                try:
                    databases = self.rate_limiter.execute_with_retry(
                        self.database_client.list_databases,
                        compartment_id=db_home.compartment_id,
                        db_home_id=db_home.id
                    ).data
                    
                    if not databases:
                        continue
                    
                    for database in databases:
                        db_backup_infos = self.process_database_backups(database, compartment.name)
                        backup_infos.extend(db_backup_infos)
                        
                except Exception as e:
                    self.logger.error(f"Error processing DB Home {db_home.display_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error processing compartment {compartment.name}: {str(e)}")
        
        self.logger.info(f"Found {len(backup_infos)} backups in compartment: {compartment.name}")
        return backup_infos

    def save_to_csv(self, backup_infos: List[BackupInfo]):
        """Guarda los datos de backups en un archivo CSV."""
        if not backup_infos:
            self.logger.warning("No backup data to save to CSV")
            return

        fieldnames = [
            'COMPARTMENT-NAME', 'DATABASE-NAME', 'NAME-BACKUP', 'SIZE_DB(GB)',
            'TYPE', 'STATE', 'TIME-STARTED', 'TIME-ENDED', 'OCID'
        ]

        try:
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for backup_info in backup_infos:
                    writer.writerow(backup_info.to_dict())

            self.logger.info(f"Successfully saved {len(backup_infos)} backup records to: {self.csv_filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")
            raise



    def run(self) -> None:
        """Ejecuta la recolección de backups en paralelo con configuración optimizada."""
        start_time = time.time()
        self.logger.info("Starting backup collection...")
        
        compartments = self.get_all_compartments()
        if not compartments:
            self.logger.warning("No compartments found")
            return
        
        all_backup_infos = []
        
        max_workers = min(3, len(compartments))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_compartment = {
                executor.submit(self.process_compartment, comp): comp 
                for comp in compartments
            }
            
            for future in as_completed(future_to_compartment):
                compartment = future_to_compartment[future]
                try:
                    backup_infos = future.result()
                    all_backup_infos.extend(backup_infos)
                except Exception as e:
                    self.logger.error(f"Error processing compartment {compartment.name}: {str(e)}")
        
        self.save_to_csv(all_backup_infos)
        
        elapsed_time = time.time() - start_time
        self.logger.info(f"Backup collection completed in {elapsed_time:.2f} seconds")
        self.logger.info(f"Total backups found: {len(all_backup_infos)}")


if __name__ == "__main__":
    try:
        oracle_backup = OracleBackupOptimized()
        oracle_backup.run()
    except Exception as e:
        logging.error(f"Application failed: {str(e)}")
        sys.exit(1)