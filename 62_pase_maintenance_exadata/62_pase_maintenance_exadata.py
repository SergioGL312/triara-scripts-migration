import os
import logging
from datetime import datetime, timedelta
import csv
from oci.config import from_file
from oci.identity import IdentityClient
from oci.database import DatabaseClient

class ExadataMaintenanceReporter:
    def __init__(self, profile_name="DEFAULT"):
        self.base_dir = "backup-report"
        self.reports_dir = os.path.join(self.base_dir, "reports", "pase")
        self.logs_dir = os.path.join(self.base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.output_file = os.path.join(self.reports_dir, f"pase-maintenance-exadata-{current_date}.csv")
        self.log_file = os.path.join(self.logs_dir, "pase-maintenance-exadata.log")
        
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        try:
            self.config = from_file(profile_name=profile_name)
            self.identity_client = IdentityClient(self.config)
            self.database_client = DatabaseClient(self.config)
            self.logger.info("OCI SDK configurado correctamente")
        except Exception as e:
            self.logger.error(f"Error al configurar OCI SDK: {str(e)}")
            raise
        
        self.current_date = datetime.now().replace(hour=19, minute=0, second=0, microsecond=0)
        self.future_date = self.current_date + timedelta(days=7)
        
        self.logger.info(f"Fecha actual: {self.current_date}")
        self.logger.info(f"Fecha futura: {self.future_date}")

    def get_all_compartments(self, compartment_id=None):
        """Obtiene todos los compartments recursivamente desde la raíz"""
        compartments = []
        
        try:
            if compartment_id is None:
                tenancy = self.identity_client.get_tenancy(self.config["tenancy"]).data
                compartments.append(tenancy)
                compartment_id = tenancy.id
            
            child_compartments = self.identity_client.list_compartments(
                compartment_id=compartment_id,
                compartment_id_in_subtree=True
            ).data
            
            compartments.extend(child_compartments)
            self.logger.info(f"Encontrados {len(compartments)} compartments")
            
        except Exception as e:
            self.logger.error(f"Error al obtener compartments: {str(e)}")
            raise
        
        return compartments

    def get_all_exadatas(self):
        """Obtiene todas las Exadatas en todos los compartments"""
        exadatas = []
        compartments = self.get_all_compartments()
        
        for compartment in compartments:
            try:
                self.logger.info(f"Buscando Exadatas en compartment: {compartment.name}")
                exadata_list = self.database_client.list_cloud_exadata_infrastructures(
                    compartment_id=compartment.id
                ).data
                
                for exadata in exadata_list:
                    exadatas.append({
                        "id": exadata.id,
                        "name": exadata.display_name,
                        "compartment_id": compartment.id,
                        "compartment_name": compartment.name
                    })
                
                self.logger.info(f"Encontradas {len(exadata_list)} Exadatas en {compartment.name}")
                
            except Exception as e:
                self.logger.error(f"Error al buscar Exadatas en compartment {compartment.name}: {str(e)}")
                continue
        
        return exadatas

    def get_maintenance_info(self, exadata_id, compartment_id):
        """Obtiene información de mantenimiento para una Exadata"""
        try:
            maintenance_runs = self.database_client.list_maintenance_runs(
                compartment_id=compartment_id,
                target_resource_id=exadata_id
            ).data
            
            return maintenance_runs
            
        except Exception as e:
            self.logger.error(f"Error al obtener mantenimientos para Exadata {exadata_id}: {str(e)}")
            return None

    def filter_maintenance_runs(self, maintenance_runs):
        """Filtra los mantenimientos por fecha"""
        filtered = []
        
        for run in maintenance_runs:
            if not hasattr(run, 'time_scheduled'):
                continue
                
            try:
                scheduled_time = datetime.strptime(run.time_scheduled, '%Y-%m-%dT%H:%M:%S.%fZ')
                
                if self.current_date <= scheduled_time <= self.future_date:
                    mexico_time = scheduled_time - timedelta(hours=6)
                    mexico_time_str = mexico_time.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    patching_minutes = run.estimated_patching_time.total_estimated_patching_time
                    hours = patching_minutes // 60
                    minutes = patching_minutes % 60
                    patching_time = f"{hours}h {minutes}m"
                    
                    filtered.append({
                        "type": run.maintenance_subtype,
                        "scheduled_utc": run.time_scheduled,
                        "scheduled_mexico": mexico_time_str,
                        "patching_time": patching_time
                    })
                    
            except Exception as e:
                self.logger.error(f"Error al procesar mantenimiento: {str(e)}")
                continue
        
        return filtered

    def generate_report(self):
        """Genera el reporte CSV"""
        found_maintenance = False
        
        try:
            with open(self.output_file, mode='w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    "EXADATA-NAME",
                    "MAINTENANCE-TYPE",
                    "SCHEDULED-UTC",
                    "SCHEDULED-MEXICO",
                    "PATCHING-TIME"
                ])
                
                exadatas = self.get_all_exadatas()
                self.logger.info(f"Procesando {len(exadatas)} Exadatas")
                
                for exadata in exadatas:
                    self.logger.info(f"Procesando Exadata: {exadata['name']}")
                    maintenance_runs = self.get_maintenance_info(exadata["id"], exadata["compartment_id"])
                    
                    if not maintenance_runs:
                        writer.writerow([exadata["name"], "No se pudo obtener información de mantenimiento"])
                        continue
                    
                    filtered_runs = self.filter_maintenance_runs(maintenance_runs)
                    
                    if filtered_runs:
                        for run in filtered_runs:
                            writer.writerow([
                                exadata["name"],
                                run["type"],
                                run["scheduled_utc"],
                                run["scheduled_mexico"],
                                run["patching_time"]
                            ])
                        found_maintenance = True
                    else:
                        writer.writerow([exadata["name"], "No existen mantenimientos programados"])
                
                if not found_maintenance:
                    writer.writerow(["No existen mantenimientos programados en ninguna Exadata"])
                    
        except Exception as e:
            self.logger.error(f"Error al generar reporte: {str(e)}")
            raise

    def run(self):
        """Ejecuta el proceso completo"""
        self.logger.info("Iniciando generación de reporte de mantenimiento de Exadatas")
        try:
            self.generate_report()
            self.logger.info("Reporte generado exitosamente")
            return True
        except Exception as e:
            self.logger.error(f"Error durante la generación del reporte: {str(e)}")
            return False


if __name__ == "__main__":
    reporter = ExadataMaintenanceReporter(profile_name="nubeprivadaoracle")
    success = reporter.run()
    
    if success:
        print("Reporte generado exitosamente")
    else:
        print("Ocurrió un error al generar el reporte. Verifique el archivo de log.")