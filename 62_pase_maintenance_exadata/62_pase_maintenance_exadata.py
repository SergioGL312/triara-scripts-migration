import logging
from datetime import datetime, timedelta
import oci
import sys
from pathlib import Path


class ExadataMaintenanceReporter:
    def __init__(self, profile_name="nubeprivadaoracle"):
        self.profile_name = profile_name
        
        base_dir = Path("backup-report")
        
        log_dir = base_dir / "scripts" / "log"
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = log_dir / f'pase-maintenance-exadata.log'
        
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        try:
            self.config = oci.config.from_file(profile_name=profile_name)
            self.identity_client = oci.identity.IdentityClient(self.config)
            self.database_client = oci.database.DatabaseClient(self.config)
            self.logger.info(f"OCI client initialized successfully with profile: {profile_name}")
        except Exception as e:
            self.logger.error(f"Error initializing OCI client: {str(e)}")
            raise
        
        self.tenancy_id = self.config["tenancy"]
        self.exadata_info = {}
        self.compartments = []
        
        reports_dir = base_dir / "reports" / profile_name
        reports_dir.mkdir(parents=True, exist_ok=True)
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.output_file = reports_dir / f'pase-maintenance-exadata-{current_date}.csv'
        
    def list_compartments(self, parent_compartment_id):
        if self.compartments:
            return self.compartments
            
        compartments = []
        try:
            self.logger.info(f"Listing compartments from: {parent_compartment_id}")
            response = self.identity_client.list_compartments(
                parent_compartment_id,
                compartment_id_in_subtree=True,
                access_level="ANY"
            )
            
            for compartment in response.data:
                if compartment.lifecycle_state == "ACTIVE":
                    compartments.append(compartment.id)
                    self.logger.debug(f"Active compartment found: {compartment.name} - {compartment.id}")

            compartments.append(self.tenancy_id)
            self.logger.info(f"Total compartments found: {len(compartments)}")
            
            self.compartments = compartments
        except Exception as e:
            self.logger.error(f"Error getting compartments: {str(e)}")
            raise
        
        return compartments
    
    def list_exadata_infrastructures(self, compartments):
        self.logger.info("Starting search for Exadata infrastructures...")
        
        for compartment_id in compartments:
            try:
                self.logger.debug(f"Searching Exadata in compartment: {compartment_id}")
                response = self.database_client.list_cloud_exadata_infrastructures(
                    compartment_id=compartment_id
                )
                
                self.logger.debug(f"Response for compartment {compartment_id}: {len(response.data)} Exadata found")
                
                for exadata in response.data:
                    self.exadata_info[exadata.display_name] = {
                        'ocid': exadata.id,
                        'compartment_id': compartment_id,
                        'lifecycle_state': exadata.lifecycle_state
                    }
                    self.logger.info(f"Exadata found: {exadata.display_name} - {exadata.id} - State: {exadata.lifecycle_state}")
                    
            except Exception as e:
                self.logger.error(f"Error getting Exadata in compartment {compartment_id}: {str(e)}")
        
        self.logger.info(f"Total Exadata found: {len(self.exadata_info)}")
        if not self.exadata_info:
            self.logger.warning("WARNING! No Exadata infrastructures found")
    
    def get_date_range(self):
        current_date = datetime.now().replace(hour=19, minute=0, second=0, microsecond=0)
        future_date = current_date + timedelta(days=7)
        
        current_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
        future_str = future_date.strftime('%Y-%m-%dT%H:%M:%S')
        
        self.logger.info(f"Search range: {current_str} to {future_str}")
        return current_str, future_str
    
    def convert_to_mexico_time(self, utc_time_str):
        try:
            if isinstance(utc_time_str, datetime):
                utc_time = utc_time_str
            else:
                if utc_time_str.endswith('Z'):
                    utc_time_str = utc_time_str[:-1] + '+00:00'
                elif '+' not in utc_time_str and 'Z' not in utc_time_str:
                    utc_time_str += '+00:00'
                utc_time = datetime.fromisoformat(utc_time_str)
            
            mexico_time = utc_time - timedelta(hours=6)
            return mexico_time.strftime('%Y-%m-%dT%H:%M:%S')
        except Exception as e:
            self.logger.error(f"Error converting time {utc_time_str}: {str(e)}")
            return "N/A"
    
    def format_patching_time(self, minutes):
        if not minutes:
            return "N/A"
        try:
            minutes = int(minutes)
            hours = minutes // 60
            remaining_minutes = minutes % 60
            return f"{hours}h {remaining_minutes}m"
        except:
            return "N/A"

    def get_maintenance_info(self, exadata_name, exadata_data):
        maintenance_runs = []
        current_date, future_date = self.get_date_range()
        exadata_ocid = exadata_data['ocid']
        
        self.logger.info(f"=== Searching maintenance for {exadata_name} ===")
        self.logger.info(f"OCID: {exadata_ocid}")
        self.logger.info(f"Date range: {current_date} to {future_date}")
        
        try:
            for compartment_id in self.compartments:
                try:
                    self.logger.debug(f"Searching maintenance in compartment: {compartment_id}")
                    
                    response = self.database_client.list_maintenance_runs(
                        compartment_id=compartment_id,
                        target_resource_id=exadata_ocid
                    )
                    
                    self.logger.debug(f"Maintenance runs for {exadata_name} in compartment {compartment_id}: {len(response.data)}")
                    
                    for run in response.data:
                        self.logger.info(f"Maintenance found:")
                        self.logger.info(f"  - Name: {run.display_name}")
                        self.logger.info(f"  - State: {run.lifecycle_state}")
                        self.logger.info(f"  - Type: {getattr(run, 'maintenance_type', 'N/A')}")
                        self.logger.info(f"  - Subtype: {getattr(run, 'maintenance_subtype', 'N/A')}")
                        self.logger.info(f"  - Scheduled: {run.time_scheduled}")
                        
                        if run.time_scheduled:
                            if hasattr(run.time_scheduled, 'strftime'):
                                scheduled_time = run.time_scheduled.strftime('%Y-%m-%dT%H:%M:%S')
                            else:
                                scheduled_time = str(run.time_scheduled)
                            
                            self.logger.debug(f"Comparing dates:")
                            self.logger.debug(f"  Current: {current_date}")
                            self.logger.debug(f"  Scheduled: {scheduled_time}")
                            self.logger.debug(f"  Future: {future_date}")
                            
                            if current_date <= scheduled_time <= future_date:
                                mexico_time = self.convert_to_mexico_time(scheduled_time)
                                
                                patching_time = "N/A"
                                if (hasattr(run, 'estimated_patching_time') and 
                                    run.estimated_patching_time and
                                    hasattr(run.estimated_patching_time, 'total_estimated_patching_time')):
                                    minutes = run.estimated_patching_time.total_estimated_patching_time
                                    patching_time = self.format_patching_time(minutes)
                                
                                maintenance_type = "N/A"
                                if hasattr(run, 'maintenance_subtype') and run.maintenance_subtype:
                                    maintenance_type = run.maintenance_subtype
                                elif hasattr(run, 'maintenance_type') and run.maintenance_type:
                                    maintenance_type = run.maintenance_type
                                
                                maintenance_data = [
                                    exadata_name,
                                    maintenance_type,
                                    scheduled_time,
                                    mexico_time,
                                    patching_time
                                ]
                                maintenance_runs.append(maintenance_data)
                                self.logger.info(f"✓ Maintenance added to report")
                            else:
                                self.logger.info(f"Maintenance outside date range: {scheduled_time}")
                        else:
                            self.logger.warning(f"Maintenance without scheduled time: {run.display_name}")
                            
                except Exception as e:
                    self.logger.error(f"Error getting maintenance in compartment {compartment_id} for {exadata_name}: {str(e)}")
            
            self.logger.info(f"Total maintenance found for {exadata_name}: {len(maintenance_runs)}")
            return maintenance_runs
            
        except Exception as e:
            self.logger.error(f"General error getting maintenance for {exadata_name}: {str(e)}")
            return []

    def generate_report(self):
        self.logger.info("=== STARTING REPORT GENERATION ===")
        all_maintenance_runs = []
        
        try:
            compartments = self.list_compartments(self.tenancy_id)
            self.logger.info(f"Compartments to check: {len(compartments)}")
            
            self.list_exadata_infrastructures(compartments)
            
            if not self.exadata_info:
                self.logger.error("No Exadata infrastructures found. Check permissions and configuration.")
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    f.write("No Exadata infrastructures found.\n")
                    f.write("Check permissions and OCI profile configuration.\n")
                return
            
            for exadata_name, exadata_data in self.exadata_info.items():
                self.logger.info(f"Processing maintenance for {exadata_name}")
                maintenance_runs = self.get_maintenance_info(exadata_name, exadata_data)
                all_maintenance_runs.extend(maintenance_runs)
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                if not all_maintenance_runs:
                    f.write("No scheduled maintenances in the next 7 days.\n")
                    f.write(f"Exadata infrastructures checked: {len(self.exadata_info)}\n")
                    f.write(f"Compartments checked: {len(self.compartments)}\n")
                    self.logger.info("No scheduled maintenances found")
                else:
                    f.write("EXADATA-NAME,MAINTENANCE-TYPE,SCHEDULED-UTC,SCHEDULED-MEXICO,PATCHING-TIME\n")
                    
                    for run in all_maintenance_runs:
                        escaped_run = [str(item).replace('"', '""') for item in run]
                        f.write(','.join([f'"{item}"' for item in escaped_run]) + '\n')
                    
                    self.logger.info(f"Report generated successfully with {len(all_maintenance_runs)} maintenances")
            
            self.logger.info(f"Output file: {self.output_file}")
            print(f"Report generated: {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            raise


def main():
    try:
        print("Starting Exadata Maintenance Reporter...")
        reporter = ExadataMaintenanceReporter()
        reporter.generate_report()
        print("Process completed. Check log file for details.")
    except Exception as e:
        print(f"Error executing script: {str(e)}")
        raise

if __name__ == "__main__":
    main()