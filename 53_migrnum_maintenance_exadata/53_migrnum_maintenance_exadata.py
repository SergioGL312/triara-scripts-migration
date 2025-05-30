import logging
from datetime import datetime, timedelta
import oci
import sys
from pathlib import Path


class ExadataMaintenanceReporter:
    def __init__(self, profile_name):
        """
        Args:
            profile_name (str): Nombre del perfil OCI a utilizar
        """
        self.profile_name = profile_name
        
        base_dir = Path("backup-report")
        
        log_dir = base_dir / "scripts" / "log"
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = log_dir / f'migr{profile_name}-maintenance-exadata.log'
        
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self.config = oci.config.from_file(profile_name=profile_name)
        self.identity_client = oci.identity.IdentityClient(self.config)
        self.database_client = oci.database.DatabaseClient(self.config)
        
        self.tenancy_id = self.config["tenancy"]
        self.exadata_info = {}
        self.compartments = []
        
        reports_dir = base_dir / "reports" / profile_name
        reports_dir.mkdir(parents=True, exist_ok=True)
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.output_file = reports_dir / f'maintenance-exadata-{current_date}.csv'
        
    def list_compartments(self, parent_compartment_id):
        """
        Args:
            parent_compartment_id (str): OCID del compartimento padre
        Returns:
            list: Lista de OCIDs de compartimentos y subcompartimentos
        """
        if self.compartments:
            return self.compartments
            
        compartments = []
        try:
            response = self.identity_client.list_compartments(
                parent_compartment_id,
                compartment_id_in_subtree=True,
                access_level="ANY"
            )
            
            for compartment in response.data:
                if compartment.lifecycle_state == "ACTIVE":
                    compartments.append(compartment.id)

            compartments.append(self.tenancy_id)
            print(f"Compartimentos encontrados: {len(compartments)}")
            self.logger.info(f"Compartimentos encontrados: {len(compartments)}")
            
            self.compartments = compartments
        except Exception as e:
            self.logger.error(f"Error obteniendo compartimentos: {str(e)}")
        
        return compartments
    
    def list_exadata_infrastructures(self, compartments):
        """
        Args:
            compartments (list): Lista de OCIDs de compartimentos
        """
        for compartment_id in compartments:
            try:
                response = self.database_client.list_cloud_exadata_infrastructures(
                    compartment_id=compartment_id
                )
                
                for exadata in response.data:
                    self.exadata_info[exadata.display_name] = exadata.id
                    self.logger.info(f"Exadata encontrado: {exadata.display_name} - {exadata.id}")
            except Exception as e:
                self.logger.error(f"Error obteniendo Exadata en compartimento {compartment_id}: {str(e)}")
        
        self.logger.info(f"Total de Exadata encontrados: {len(self.exadata_info)}")
    
    def get_date_range(self):
        current_date = datetime.now().replace(hour=19, minute=0, second=0, microsecond=0)
        future_date = current_date + timedelta(days=7)
        return current_date.strftime('%Y-%m-%dT%H:%M:%S'), future_date.strftime('%Y-%m-%dT%H:%M:%S')
    
    def convert_to_mexico_time(self, utc_time_str):
        utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%S%z')
        mexico_time = utc_time - timedelta(hours=6)
        return mexico_time.strftime('%Y-%m-%dT%H:%M:%S')
    
    def format_patching_time(self, minutes):
        hours = minutes // 60
        remaining_minutes = minutes % 60
        return f"{hours}h {remaining_minutes}m"

    def get_maintenance_info(self, exadata_name, exadata_ocid):
        maintenance_runs = []
        current_date, future_date = self.get_date_range()
        self.logger.info(f"Buscando mantenimientos para {exadata_name} desde {current_date} hasta {future_date}")
        
        try:
            for compartment_id in self.compartments:
                try:
                    response = self.database_client.list_maintenance_runs(
                        compartment_id=compartment_id,
                        target_resource_id=exadata_ocid
                    )
                    
                    self.logger.info(f"Mantenimientos encontrados en compartimento {compartment_id}: {len(response.data)}")
                    
                    for run in response.data:
                        self.logger.info(f"Mantenimiento encontrado: {run.display_name}, Estado: {run.lifecycle_state}, Programado: {run.time_scheduled}")
                        
                        if run.lifecycle_state != "SCHEDULED":
                            self.logger.info(f"Omitiendo mantenimiento porque el estado es {run.lifecycle_state} en lugar de SCHEDULED")
                            continue

                        if run.time_scheduled:
                            scheduled_time = run.time_scheduled.isoformat()

                            if current_date <= scheduled_time <= future_date:
                                mexico_time = "N/A"
                                try:
                                    mexico_time = self.convert_to_mexico_time(scheduled_time)
                                except Exception as e:
                                    self.logger.error(f"Error convirtiendo tiempo: {str(e)}")
                                
                                patching_time = "N/A"
                                if run.estimated_patching_time and run.estimated_patching_time.total_estimated_patching_time:
                                    minutes = run.estimated_patching_time.total_estimated_patching_time
                                    patching_time = self.format_patching_time(minutes)
                                
                                maintenance_data = [
                                    exadata_name,
                                    run.maintenance_subtype or "N/A",
                                    scheduled_time,
                                    mexico_time,
                                    patching_time
                                ]
                                maintenance_runs.append(maintenance_data)
                            else:
                                self.logger.info(f"Omitiendo mantenimiento porque la fecha {scheduled_time} está fuera del rango {current_date} - {future_date}")
                except Exception as e:
                    self.logger.error(f"Error al obtener mantenimiento en compartimento {compartment_id} para {exadata_name}: {str(e)}")
            
            return maintenance_runs
        except Exception as e:
            self.logger.error(f"Error general al obtener mantenimiento para {exadata_name}: {str(e)}")
            return []    

    def generate_report(self):
        all_maintenance_runs = []
        
        compartments = self.list_compartments(self.tenancy_id)
        self.list_exadata_infrastructures(compartments)
        
        if not self.exadata_info:
            self.logger.warning("No se encontraron infraestructuras Exadata. Verifica permisos y configuración.")
        
        for exadata_name, exadata_ocid in self.exadata_info.items():
            self.logger.info(f"Procesando mantenimiento para {exadata_name}")
            maintenance_runs = self.get_maintenance_info(exadata_name, exadata_ocid)
            all_maintenance_runs.extend(maintenance_runs)
        
        with open(self.output_file, 'w') as f:
            if not all_maintenance_runs:
                f.write("No existen mantenimientos programados.\n")
                self.logger.info("No se encontraron mantenimientos programados")
                return
            
            f.write("EXADATA-NAME,MAINTENANCE-TYPE,SCHEDULED-UTC,SCHEDULED-MEXICO,PATCHING-TIME\n")
            
            for run in all_maintenance_runs:
                f.write(','.join([f'"{item}"' for item in run]) + '\n')
            
            self.logger.info(f"Reporte generado exitosamente: {self.output_file}")


def main():
    if len(sys.argv) != 2:
        print("⚠️ No se proporcionó un tenant. Usando el tenant por defecto ('DEFAULT').")
        profile_name = "DEFAULT"
    else:
        profile_name = sys.argv[1]
    
    reporter = ExadataMaintenanceReporter(profile_name)
    reporter.generate_report()

if __name__ == "__main__":
    main()