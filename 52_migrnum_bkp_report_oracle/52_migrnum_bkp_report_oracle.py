import oci
import time
import csv
import os
import logging
import sys
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

class OracleBackup:
    def __init__(self, tenant="trmxmigrnum", config_file="~/.oci/config"):
        self.config = oci.config.from_file(config_file, tenant)
        self.identity_client = oci.identity.IdentityClient(self.config)
        self.database_client = oci.database.DatabaseClient(self.config)
        self.tenancy_id = self.config["tenancy"]

        self.tenant_name = self.identity_client.get_tenancy(self.tenancy_id).data.name

        self.now_utc = datetime.now(timezone.utc)
        self.yesterday_start = self.now_utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        self.yesterday_end = self.yesterday_start + timedelta(days=1)
        
        self.base_dir = os.path.join(os.getcwd(), "backup-report")
        self.reports_dir = os.path.join(self.base_dir, "reports", tenant)
        self.logs_dir = os.path.join(self.base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        self.log_filename = os.path.join(self.logs_dir, f"migr{tenant}-backups-db-oracle.log")

        self.logger = logging.getLogger("oracle_backup_logger")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if self.logger.handlers:
            self.logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_filename)
        console_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.csv_filename = os.path.join(self.reports_dir, f"backups-db-oracle-{self.yesterday_start.date()}.csv")

    def get_all_compartments(self):
        """Obtiene todos los compartimentos activos del tenancy."""
        compartments = self.identity_client.list_compartments(
            compartment_id=self.tenancy_id,
            compartment_id_in_subtree=True,
            access_level="ANY"
        ).data
        return [c for c in compartments if c.lifecycle_state == "ACTIVE"]

    def get_all_db_homes(self, compartment_id, retries=10, delay=10):
        """Obtiene todos los DB Homes dentro de un compartimento, con reintentos en caso de error 429 (Too Many Requests)."""
        attempt = 0
        while attempt < retries:
            try:
                return self.database_client.list_db_homes(compartment_id=compartment_id).data
            except oci.exceptions.ServiceError as e:
                if e.status == 429:
                    self.logger.warning(f"Error 429: Demasiadas solicitudes. Reintentando en {delay} segundos...")
                    attempt += 1
                    time.sleep(delay)
                else:
                    self.logger.error(f"Error al obtener DB Homes: {str(e)}")
                    break
        return []

    def get_databases_from_home(self, db_home_id, compartment_id):
        """Obtiene todas las bases de datos dentro de un DB Home."""
        try:
            return self.database_client.list_databases(
                compartment_id=compartment_id,
                db_home_id=db_home_id
            ).data
        except Exception as e:
            self.logger.error(f"Error al obtener bases de datos del DB Home {db_home_id}: {str(e)}")
            return []

    def get_backups(self, database_id):
        """Obtiene los backups de una base de datos finalizados en la fecha de ayer."""
        try:
            backups = self.database_client.list_backups(database_id=database_id).data
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
                        'OCID': backup.id
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
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(self.process_and_save_data, compartments))

        all_backup_data = [item for result in results for item in result]
        self.save_to_csv(all_backup_data)

if __name__ == "__main__":
    oracle_backup = OracleBackup()
    oracle_backup.run()