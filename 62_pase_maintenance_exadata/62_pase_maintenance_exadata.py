import os
import logging
from datetime import datetime, timedelta
import csv
from oci.config import from_file
from oci.identity import IdentityClient
from oci.database import DatabaseClient
from oci.exceptions import ServiceError

class ExadataMaintenanceReporter:
    def __init__(self, profile_name="DEFAULT"):
        # Configuración de directorios
        self.base_dir = "backup-report"
        self.reports_dir = os.path.join(self.base_dir, "reports", "pase")
        self.logs_dir = os.path.join(self.base_dir, "scripts", "log")
        
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
        
        # Configuración de archivos de salida
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.output_file = os.path.join(self.reports_dir, f"pase-maintenance-exadata-{current_date}.csv")
        self.log_file = os.path.join(self.logs_dir, "pase-maintenance-exadata.log")
        
        # Configuración de logging
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Configuración de fechas
        self.current_date = datetime.now().replace(hour=19, minute=0, second=0, microsecond=0)
        self.future_date = self.current_date + timedelta(days=7)
        
        # Inicialización de clientes OCI
        try:
            self.config = from_file(profile_name=profile_name)
            self.identity_client = IdentityClient(self.config)
            self.database_client = DatabaseClient(self.config)
            self.logger.info("OCI SDK configurado correctamente")
        except Exception as e:
            self.logger.error(f"Error al configurar OCI SDK: {str(e)}")
            raise
        
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
            
        except ServiceError as e:
            self.logger.error(f"Error al obtener compartments: {str(e)}")
            raise
        
        return compartments

    def get_cloud_at_customer_exadatas(self, compartment_id):
        """Obtiene específicamente Exadata Cloud@Customer"""
        try:
            # Listar todas las Exadata Infrastructures
            all_exadatas = self.database_client.list_cloud_exadata_infrastructures(
                compartment_id=compartment_id
            ).data
            
            # Filtrar solo las que son Cloud@Customer
            cloud_at_customer = []
            for exadata in all_exadatas:
                if hasattr(exadata, 'infrastructure_type'):
                    if exadata.infrastructure_type == 'CLOUD_AT_CUSTOMER':
                        cloud_at_customer.append(exadata)
                elif "CLOUD_AT_CUSTOMER" in exadata.display_name.upper():
                    cloud_at_customer.append(exadata)
            
            return cloud_at_customer
            
        except ServiceError as e:
            self.logger.error(f"Error al buscar Exadatas Cloud@Customer: {str(e)}")
            return []

    def get_all_exadatas(self):
        """Obtiene todas las Exadatas Cloud@Customer en todos los compartments"""
        exadatas = []
        compartments = self.get_all_compartments()
        
        for compartment in compartments:
            try:
                self.logger.info(f"Buscando Exadatas Cloud@Customer en compartment: {compartment.name}")
                
                # Obtener solo Exadata Cloud@Customer
                exadata_list = self.get_cloud_at_customer_exadatas(compartment.id)
                
                for exadata in exadata_list:
                    exadatas.append({
                        "id": exadata.id,
                        "name": exadata.display_name,
                        "compartment_id": compartment.id,
                        "compartment_name": compartment.name,
                        "type": "CLOUD_AT_CUSTOMER"
                    })
                
                self.logger.info(f"Encontradas {len(exadata_list)} Exadatas Cloud@Customer en {compartment.name}")
                
            except Exception as e:
                self.logger.error(f"Error al buscar Exadatas en compartment {compartment.name}: {str(e)}")
                continue
        
        return exadatas

    def get_maintenance_info(self, exadata_id, compartment_id):
        """Obtiene información de mantenimiento para una Exadata"""
        try:
            maintenance_runs = self.database_client.list_maintenance_runs(
                compartment_id=compartment_id,
                target_resource_id=exadata_id,
                target_resource_type="CLOUD_EXADATA_INFRASTRUCTURE"
            ).data
            
            return maintenance_runs
            
        except ServiceError as e:
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
                        "patching_time": patching_time,
                        "description": run.description
                    })
                    
            except Exception as e:
                self.logger.error(f"Error al procesar mantenimiento: {str(e)}")
                continue
        
        return filtered

    def generate_report(self):
        """Genera el reporte CSV específico para Cloud@Customer"""
        found_maintenance = False
        
        try:
            with open(self.output_file, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    "EXADATA-NAME",
                    "TIPO-EXADATA",
                    "COMPARTMENT",
                    "TIPO-MANTENIMIENTO",
                    "PROGRAMADO-UTC",
                    "PROGRAMADO-MEXICO",
                    "TIEMPO-PARCHE",
                    "DESCRIPCION"
                ])
                
                exadatas = self.get_all_exadatas()
                self.logger.info(f"Procesando {len(exadatas)} Exadatas Cloud@Customer")
                
                if not exadatas:
                    writer.writerow(["No se encontraron Exadatas Cloud@Customer"])
                    return
                
                for exadata in exadatas:
                    self.logger.info(f"Procesando Exadata Cloud@Customer: {exadata['name']}")
                    maintenance_runs = self.get_maintenance_info(exadata["id"], exadata["compartment_id"])
                    
                    if not maintenance_runs:
                        writer.writerow([
                            exadata["name"],
                            exadata["type"],
                            exadata["compartment_name"],
                            "No se pudo obtener información de mantenimiento"
                        ])
                        continue
                    
                    filtered_runs = self.filter_maintenance_runs(maintenance_runs)
                    
                    if filtered_runs:
                        for run in filtered_runs:
                            writer.writerow([
                                exadata["name"],
                                exadata["type"],
                                exadata["compartment_name"],
                                run["type"],
                                run["scheduled_utc"],
                                run["scheduled_mexico"],
                                run["patching_time"],
                                run["description"]
                            ])
                        found_maintenance = True
                    else:
                        writer.writerow([
                            exadata["name"],
                            exadata["type"],
                            exadata["compartment_name"],
                            "No existen mantenimientos programados"
                        ])
                
                if not found_maintenance:
                    writer.writerow(["No existen mantenimientos programados para las Exadatas Cloud@Customer"])
                    
        except Exception as e:
            self.logger.error(f"Error al generar reporte: {str(e)}")
            raise

    def run(self):
        """Ejecuta el proceso completo"""
        self.logger.info("Iniciando generación de reporte de mantenimiento para Exadata Cloud@Customer")
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
        print(f"Reporte de Exadata Cloud@Customer generado exitosamente en: {reporter.output_file}")
        print(f"Detalles del reporte:")
        print(f"- Exadatas procesadas: Cloud@Customer exclusivamente")
        print(f"- Rango de fechas analizado: {reporter.current_date} a {reporter.future_date}")
    else:
        print(f"Ocurrió un error al generar el reporte. Verifique el archivo de log: {reporter.log_file}")
        print("Posibles causas:")
        print("- Permisos insuficientes para leer Exadata Cloud@Customer")
        print("- Problemas de conexión con la API de OCI")
        print("- Configuración incorrecta del perfil OCI")