import oci
import csv
import os
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import threading

class OracleBackup:
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
        self.min_request_interval = 0.5
        self.max_retries = 3
        self.backoff_factor = 2

    def rate_limited_request(self, func, *args, **kwargs):
        """Ejecuta una función con rate limiting y retry logic."""
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
                        self.logger.warning(f"Rate limit alcanzado. Esperando {wait_time}s antes del intento {attempt + 1}")
                        time.sleep(wait_time)
                        if attempt == self.max_retries - 1:
                            raise
                    else:
                        raise
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(1)

    def get_all_compartments(self):
        """Obtiene todos los compartimentos activos del tenancy."""
        compartments = self.rate_limited_request(
            self.identity_client.list_compartments,
            compartment_id=self.tenancy_id,
            compartment_id_in_subtree=True,
            access_level="ANY"
        ).data
        return [c for c in compartments if c.lifecycle_state == "ACTIVE"]

    def get_all_db_homes(self, compartment_id):
        """Obtiene todos los DB Homes dentro de un compartimento."""
        try:
            return self.rate_limited_request(
                self.database_client.list_db_homes,
                compartment_id=compartment_id
            ).data
        except Exception as e:
            self.logger.error(f"Error al obtener DB Homes: {str(e)}")
            return []

    def get_databases_from_home(self, db_home_id, compartment_id):
        """Obtiene todas las bases de datos dentro de un DB Home."""
        try:
            return self.rate_limited_request(
                self.database_client.list_databases,
                compartment_id=compartment_id,
                db_home_id=db_home_id
            ).data
        except Exception as e:
            self.logger.error(f"Error al obtener bases de datos del DB Home {db_home_id}: {str(e)}")
            return []

    def get_backups(self, database_id):
        """Obtiene los backups de una base de datos finalizados en la fecha de ayer."""
        try:
            backups = self.rate_limited_request(
                self.database_client.list_backups,
                database_id=database_id
            ).data
            return [
                b for b in backups if b.time_ended and self.yesterday_start <= b.time_ended < self.yesterday_end
            ]
        except Exception as e:
            self.logger.error(f"Error al obtener backups para Database {database_id}: {str(e)}")
            return []

    def process_and_save_data(self, compartment):
        """Procesa bases de datos y respalda información."""
        backup_data = []
        db_homes = self.get_all_db_homes(compartment.id)

        if not db_homes:
            self.logger.info(f"No se encontraron DB Homes en el compartimento: {compartment.name}")
            return backup_data

        for db_home in db_homes:
            databases = self.get_databases_from_home(db_home.id, db_home.compartment_id)

            if not databases:
                self.logger.info(f"No se encontraron bases de datos en el DB Home {db_home.display_name}.")
                continue

            for database in databases:
                backups = self.get_backups(database.id)

                if not backups:
                    self.logger.info(f"No se encontraron backups para la base de datos {database.db_name}.")
                    continue

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

        return backup_data

    def save_to_csv(self, data):
        """Guarda los datos de los backups en un archivo CSV."""
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

    def run(self):
        """Ejecuta la recolección de backups y los guarda en un CSV."""
        compartments = self.get_all_compartments()
        max_workers = min(2, len(compartments))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.process_and_save_data, compartments))

        all_backup_data = [item for result in results for item in result]
        self.save_to_csv(all_backup_data)

    def run_sequential(self):
        """Ejecuta la recolección de backups de forma secuencial (más lento pero más seguro)."""
        compartments = self.get_all_compartments()
        all_backup_data = []
        
        for compartment in compartments:
            self.logger.info(f"Procesando compartimento: {compartment.name}")
            backup_data = self.process_and_save_data(compartment)
            all_backup_data.extend(backup_data)
            time.sleep(1)
        
        self.save_to_csv(all_backup_data)

if __name__ == "__main__":
    oracle_backup = OracleBackup()
    oracle_backup.run()