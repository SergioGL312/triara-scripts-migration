import oci
import csv
import os
import logging
import sys
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

class MysqlBackup:
    def __init__(self, tenant="trmxmigrnum", config_file="~/.oci/config"):
        self.config = oci.config.from_file(config_file, tenant)
        self.identity_client = oci.identity.IdentityClient(self.config)
        self.mysql_client = oci.mysql.DbSystemClient(self.config)
        self.backup_client = oci.mysql.DbBackupsClient(self.config)
        self.tenancy_id = self.config["tenancy"]
        
        try:
            self.tenant_name = self.identity_client.get_tenancy(self.tenancy_id).data.name
        except Exception as e:
            print(f"Error retrieving tenant name: {str(e)}")
            self.tenant_name = "UNKNOWN_TENANT"

        self.base_dir = os.path.join(os.getcwd(), "backup-report")
        self.reports_dir = os.path.join(self.base_dir, "reports", tenant)
        self.logs_dir = os.path.join(self.base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        self.log_file = os.path.join(self.logs_dir, f"{tenant}-backups-db-mysql.log")
        
        self.logger = logging.getLogger("mysql_backup_logger")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if self.logger.handlers:
            self.logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_file)
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        yesterday_str = self.yesterday.strftime("%Y-%m-%d")
        self.output_file = os.path.join(self.reports_dir, f"backups-db-mysql-{yesterday_str}.csv")
        
        self.headers = [
            "COMPARTMENT-NAME", "DATABASE-NAME", "LAST-BACKUP-NAME",
            "SIZE_DB(GB)", "TYPE", "STATE", "TIME-STARTED", "OCID"
        ]

    def fetch_db_backups(self, compartment):
        backups_data = []
        
        try:
            self.logger.info(f"Fetching database instances for compartment: {compartment.name}")
            
            db_instances = []
            page = None
            while True:
                if page is None:
                    response = self.mysql_client.list_db_systems(
                        compartment_id=compartment.id,
                        limit=100
                    )
                else:
                    response = self.mysql_client.list_db_systems(
                        compartment_id=compartment.id,
                        limit=100,
                        page=page
                    )
                
                db_instances.extend(response.data)
                page = response.next_page if response.has_next_page else None
                if not response.has_next_page:
                    break
            
            for db in db_instances:
                db_id = db.id
                db_name = db.display_name
                
                try:
                    self.logger.info(f"Fetching backups for database: {db_name}")
                    
                    backups = []
                    page = None
                    while True:
                        if page is None:
                            response = self.backup_client.list_backups(
                                compartment_id=compartment.id,
                                db_system_id=db_id
                            )
                        else:
                            response = self.backup_client.list_backups(
                                compartment_id=compartment.id,
                                db_system_id=db_id,
                                page=page
                            )
                        
                        backups.extend(response.data)
                        page = response.next_page if response.has_next_page else None
                        if not response.has_next_page:
                            break
                except Exception as e:
                    self.logger.error(f"Error retrieving backups for database {db_name}: {str(e)}")
                    continue

                if not backups:
                    self.logger.warning(f"No backups found for database: {db_name}")
                    continue
                
                backups = [
                    backup for backup in backups 
                    if backup.time_created.date() == self.yesterday
                ]
                
                if not backups:
                    self.logger.warning(f"No backups found for database {db_name} on {self.yesterday}")
                    continue
                
                backups = sorted(backups, key=lambda b: b.time_created, reverse=True)
                
                for backup in backups:
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
        except Exception as e:
            self.logger.error(f"Error processing compartment {compartment.name}: {str(e)}")
        
        return backups_data

    def fetch_backups(self):
        self.logger.info(f"Starting backup report generation for {self.yesterday}")
        
        try:
            compartments = self.identity_client.list_compartments(
                self.tenancy_id, compartment_id_in_subtree=True
            ).data
            compartments = [c for c in compartments if c.lifecycle_state == "ACTIVE"]
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(self.fetch_db_backups, compartments))
            
            flat_results = [item for sublist in results for item in sublist]
            
            with open(self.output_file, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(self.headers)
                writer.writerows(flat_results)
            
            self.logger.info(f"CSV file successfully generated: {self.output_file}")
            print(f"✅ CSV file successfully generated: {self.output_file}")
            print(f"Log file stored at: {self.log_file}")
        except Exception as e:
            self.logger.critical(f"Critical error during backup report generation: {str(e)}")
            print(f"❌ Error: {str(e)}")
            print(f"📝 See log for details: {self.log_file}")

if __name__ == "__main__":
    mysql_backup = MysqlBackup()
    mysql_backup.fetch_backups()