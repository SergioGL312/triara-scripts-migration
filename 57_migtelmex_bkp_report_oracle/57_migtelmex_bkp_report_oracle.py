import oci
import csv
import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import List, Dict, Any

class OracleBackupOptimized:
    def __init__(self, config_file="~/.oci/config", profile="trmxmigtelmex"):
        self.config = oci.config.from_file(config_file, profile)
        self.identity_client = oci.identity.IdentityClient(self.config)
        self.database_client = oci.database.DatabaseClient(self.config)
        self.tenancy_id = self.config["tenancy"]

        self.now_utc = datetime.now(timezone.utc)
        self.yesterday_start = self.now_utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        self.yesterday_end = self.yesterday_start + timedelta(days=1)
        
        base_dir = os.path.join(os.getcwd(), "backup-report")
        self.reports_dir = os.path.join(base_dir, "reports", "telmex")
        self.logs_dir = os.path.join(base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        self.log_filename = os.path.join(self.logs_dir, f"migtelmex-backups-db-oracle.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_filename),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

        self.csv_filename = os.path.join(self.reports_dir, f"backups-db-oracle-{self.yesterday_start.date()}.csv")
        
        self.request_lock = threading.Lock()
        self.last_request_time = 0
        self.min_request_interval = 0.1
        self.max_retries = 3
        self.backoff_factor = 1.5
        
        self.compartments_cache = None
        self.db_homes_cache = {}
        self.databases_cache = {}

    def rate_limited_request(self, func, *args, **kwargs):
        with self.request_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_request_interval:
                time.sleep(self.min_request_interval - time_since_last)
            
            for attempt in range(self.max_retries):
                try:
                    result = func(*args, **kwargs)
                    self.last_request_time = time.time()
                    return result
                except oci.exceptions.ServiceError as e:
                    if e.status == 429:
                        wait_time = self.backoff_factor ** attempt
                        self.logger.warning(f"Rate limit alcanzado. Esperando {wait_time:.1f}s antes del intento {attempt + 1}")
                        time.sleep(wait_time)
                        if attempt == self.max_retries - 1:
                            raise
                    elif e.status >= 500:
                        wait_time = self.backoff_factor ** attempt
                        self.logger.warning(f"Error del servidor ({e.status}). Reintentando en {wait_time:.1f}s")
                        time.sleep(wait_time)
                        if attempt == self.max_retries - 1:
                            raise
                    else:
                        raise
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        self.logger.error(f"Error tras {self.max_retries} intentos: {str(e)}")
                        raise
                    time.sleep(0.5)

    def get_all_compartments(self):
        if self.compartments_cache is not None:
            return self.compartments_cache
            
        self.logger.info("Obteniendo lista de compartimentos...")
        compartments = self.rate_limited_request(
            self.identity_client.list_compartments,
            compartment_id=self.tenancy_id,
            compartment_id_in_subtree=True,
            access_level="ANY"
        ).data
        
        self.compartments_cache = [c for c in compartments if c.lifecycle_state == "ACTIVE"]
        self.logger.info(f"Encontrados {len(self.compartments_cache)} compartimentos activos")
        return self.compartments_cache

    def get_all_db_homes_batch(self, compartment_ids: List[str]) -> Dict[str, List]:
        db_homes_by_compartment = {}
        
        for compartment_id in compartment_ids:
            if compartment_id in self.db_homes_cache:
                db_homes_by_compartment[compartment_id] = self.db_homes_cache[compartment_id]
                continue
                
            try:
                db_homes = self.rate_limited_request(
                    self.database_client.list_db_homes,
                    compartment_id=compartment_id
                ).data
                
                self.db_homes_cache[compartment_id] = db_homes
                db_homes_by_compartment[compartment_id] = db_homes
                
            except Exception as e:
                self.logger.error(f"Error al obtener DB Homes del compartimento {compartment_id}: {str(e)}")
                db_homes_by_compartment[compartment_id] = []
                
        return db_homes_by_compartment

    def get_databases_batch(self, db_home_compartment_pairs: List[tuple]) -> Dict[str, List]:
        databases_by_home = {}
        
        for db_home_id, compartment_id in db_home_compartment_pairs:
            cache_key = f"{db_home_id}_{compartment_id}"
            
            if cache_key in self.databases_cache:
                databases_by_home[db_home_id] = self.databases_cache[cache_key]
                continue
                
            try:
                databases = self.rate_limited_request(
                    self.database_client.list_databases,
                    compartment_id=compartment_id,
                    db_home_id=db_home_id
                ).data
                
                self.databases_cache[cache_key] = databases
                databases_by_home[db_home_id] = databases
                
            except Exception as e:
                self.logger.error(f"Error al obtener bases de datos del DB Home {db_home_id}: {str(e)}")
                databases_by_home[db_home_id] = []
                
        return databases_by_home

    def get_backups_with_time_filter(self, database_id: str) -> List:
        try:
            backups = self.rate_limited_request(
                self.database_client.list_backups,
                database_id=database_id,
                time_ended_greater_than_or_equal_to=self.yesterday_start,
                time_ended_less_than=self.yesterday_end
            ).data
            
            return [
                b for b in backups 
                if b.time_ended and self.yesterday_start <= b.time_ended < self.yesterday_end
            ]
            
        except TypeError:
            try:
                backups = self.rate_limited_request(
                    self.database_client.list_backups,
                    database_id=database_id
                ).data
                
                return [
                    b for b in backups 
                    if b.time_ended and self.yesterday_start <= b.time_ended < self.yesterday_end
                ]
            except Exception as e:
                self.logger.error(f"Error al obtener backups para Database {database_id}: {str(e)}")
                return []

    def process_compartment(self, compartment) -> List[Dict[str, Any]]:
        backup_data = []
        
        try:
            db_homes_dict = self.get_all_db_homes_batch([compartment.id])
            db_homes = db_homes_dict.get(compartment.id, [])

            if not db_homes:
                self.logger.debug(f"No se encontraron DB Homes en el compartimento: {compartment.name}")
                return backup_data

            db_home_pairs = [(db_home.id, db_home.compartment_id) for db_home in db_homes]
            
            databases_dict = self.get_databases_batch(db_home_pairs)

            for db_home in db_homes:
                databases = databases_dict.get(db_home.id, [])
                
                if not databases:
                    self.logger.debug(f"No se encontraron bases de datos en el DB Home {db_home.display_name}")
                    continue

                for database in databases:
                    backups = self.get_backups_with_time_filter(database.id)

                    for backup in backups:
                        backup_info = {
                            'COMPARTMENT-NAME': compartment.name,
                            'DATABASE-NAME': database.db_name,
                            'NAME-BACKUP': backup.display_name,
                            'SIZE_DB(GB)': getattr(backup, 'database_size_in_gbs', 'N/A'),
                            'TYPE': getattr(backup, 'type', 'N/A'),
                            'STATE': backup.lifecycle_state,
                            'TIME-STARTED': backup.time_started,
                            'TIME-ENDED': getattr(backup, 'time_ended', 'N/A'),
                            'OCID': backup.id,
                        }
                        backup_data.append(backup_info)

        except Exception as e:
            self.logger.error(f"Error procesando compartimento {compartment.name}: {str(e)}")

        return backup_data

    def save_to_csv(self, data: List[Dict[str, Any]]):
        if not data:
            self.logger.warning("No hay datos para guardar en el CSV")
            return

        fieldnames = ['COMPARTMENT-NAME', 'DATABASE-NAME', 'NAME-BACKUP', 'SIZE_DB(GB)',
                      'TYPE', 'STATE', 'TIME-STARTED', 'TIME-ENDED', 'OCID']

        with open(self.csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        self.logger.info(f"Datos guardados en el archivo: {self.csv_filename}")
        self.logger.info(f"Total de backups encontrados: {len(data)}")

    def run_optimized(self):
        start_time = time.time()
        self.logger.info(f"Iniciando recolección de backups para el día: {self.yesterday_start.date()}")
        
        compartments = self.get_all_compartments()
        
        if not compartments:
            self.logger.warning("No se encontraron compartimentos")
            return

        max_workers = min(4, len(compartments), os.cpu_count() or 2)
        self.logger.info(f"Procesando {len(compartments)} compartimentos con {max_workers} workers")
        
        all_backup_data = []
        processed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_compartment = {
                executor.submit(self.process_compartment, comp): comp 
                for comp in compartments
            }
            
            for future in as_completed(future_to_compartment):
                compartment = future_to_compartment[future]
                try:
                    result = future.result()
                    all_backup_data.extend(result)
                    processed_count += 1
                    
                    self.logger.info(
                        f"Procesado {processed_count}/{len(compartments)} compartimentos. "
                        f"Compartimento: {compartment.name}, Backups encontrados: {len(result)}"
                    )
                    
                except Exception as e:
                    self.logger.error(f"Error procesando compartimento {compartment.name}: {str(e)}")

        self.save_to_csv(all_backup_data)
        
        elapsed_time = time.time() - start_time
        self.logger.info(f"Proceso completado en {elapsed_time:.2f} segundos")
        self.logger.info(f"Total de backups del día anterior: {len(all_backup_data)}")

    def run_sequential_optimized(self):
        start_time = time.time()
        self.logger.info(f"Iniciando recolección secuencial de backups para el día: {self.yesterday_start.date()}")
        
        compartments = self.get_all_compartments()
        all_backup_data = []
        
        for i, compartment in enumerate(compartments, 1):
            self.logger.info(f"Procesando compartimento {i}/{len(compartments)}: {compartment.name}")
            backup_data = self.process_compartment(compartment)
            all_backup_data.extend(backup_data)
            
            self.logger.info(f"Compartimento {compartment.name}: {len(backup_data)} backups encontrados")
            
            if i < len(compartments):
                time.sleep(0.2)
        
        self.save_to_csv(all_backup_data)
        
        elapsed_time = time.time() - start_time
        self.logger.info(f"Proceso secuencial completado en {elapsed_time:.2f} segundos")

    def run(self):
        self.run_optimized()

    def run_sequential(self):
        self.run_sequential_optimized()

if __name__ == "__main__":
    oracle_backup = OracleBackupOptimized()
    oracle_backup.run()