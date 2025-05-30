import time
import oci
import csv
import os
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

class MysqlBackup:
    def __init__(self, config_file="~/.oci/config", profile="trmxmigtelmex"):
        self.config = oci.config.from_file(config_file, profile)
        self.identity_client = oci.identity.IdentityClient(self.config)
        self.mysql_client = oci.mysql.DbSystemClient(self.config)
        self.backup_client = oci.mysql.DbBackupsClient(self.config)
        self.tenancy_id = self.config["tenancy"]

        base_dir = os.path.join(os.getcwd(), "backup-report")
        self.reports_dir = os.path.join(base_dir, "reports", "telmex")
        self.logs_dir = os.path.join(base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        self.log_file = os.path.join(self.logs_dir, "migtelmex-backups-db-mysql.log")
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger()
        self.logger.addHandler(logging.StreamHandler())

        self.yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        yesterday_str = self.yesterday.strftime("%Y-%m-%d")
        self.output_file = os.path.join(self.reports_dir, f"backups-db-mysql-{yesterday_str}.csv")
        
        self.headers = [
            "COMPARTMENT-NAME", "DATABASE-NAME", "LAST-BACKUP-NAME",
            "SIZE_DB(GB)", "TYPE", "STATE", "TIME-STARTED", "OCID"
        ]

    def fetch_db_backups(self, compartment):
        db_instances = self.mysql_client.list_db_systems(compartment_id=compartment.id).data
        backups_data = []

        for db in db_instances:
            db_id = db.id
            db_name = db.display_name

            try:
                backups = self.backup_client.list_backups(
                    compartment_id=compartment.id,
                    db_system_id=db_id
                ).data
            except Exception as e:
                self.logger.error(f"Error obteniendo los respaldos de la base de datos {db_name}: {e}")
                backups = []

            backups = sorted(backups, key=lambda b: b.time_created, reverse=True)

            for backup in backups:
                backup_date = backup.time_created.date()
                if backup_date == self.yesterday:
                    backups_data.append([
                        compartment.name,
                        db_name,
                        backup.display_name,
                        backup.data_storage_size_in_gbs,
                        backup.backup_type,
                        backup.lifecycle_state,
                        backup.time_created.strftime("%Y-%m-%d %H:%M:%S"),
                        backup.id,
                    ])
                    self.logger.info(f"Respaldo del {backup_date} de la base de datos {db_name} registrado correctamente")
                    break
        
        return backups_data

    def fetch_backups(self):
        start_time = time.time()

        compartments = self.identity_client.list_compartments(
            self.tenancy_id, compartment_id_in_subtree=True
        ).data
        
        compartments = [c for c in compartments if c.lifecycle_state == "ACTIVE"]

        with ThreadPoolExecutor() as executor:
            all_backups = []
            for compartment in compartments:
                all_backups.append(executor.submit(self.fetch_db_backups, compartment))

            results = [future.result() for future in all_backups]

        with open(self.output_file, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(self.headers)
            
            for backup_data in results:
                for row in backup_data:
                    writer.writerow(row)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        self.logger.info(f"Tiempo de ejecución: {execution_time:.2f} segundos")
        self.logger.info(f"Archivo CSV generado exitosamente: {self.output_file}")
        
        print(f"Tiempo de ejecución: {execution_time:.2f} segundos")
        print(f"✅ Archivo CSV generado exitosamente: {self.output_file}")
        print(f"Log de eventos almacenado en: {self.log_file}")


if __name__ == "__main__":
    mysql_backup = MysqlBackup()
    mysql_backup.fetch_backups()