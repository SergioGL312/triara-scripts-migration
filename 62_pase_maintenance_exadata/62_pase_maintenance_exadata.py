import logging
from datetime import datetime, timedelta
import oci
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
            level=logging.DEBUG,
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
            self.logger.info(f"Cliente OCI inicializado correctamente con perfil: {profile_name}")
        except Exception as e:
            self.logger.error(f"Error inicializando cliente OCI: {str(e)}")
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
            self.logger.info(f"Listando compartimentos desde: {parent_compartment_id}")
            response = self.identity_client.list_compartments(
                parent_compartment_id,
                compartment_id_in_subtree=True,
                access_level="ANY"
            )
            
            for compartment in response.data:
                if compartment.lifecycle_state == "ACTIVE":
                    compartments.append(compartment.id)
                    self.logger.debug(f"Compartimento activo encontrado: {compartment.name} - {compartment.id}")

            compartments.append(self.tenancy_id)
            self.logger.info(f"Compartimentos encontrados: {len(compartments)}")
            
            self.compartments = compartments
        except Exception as e:
            self.logger.error(f"Error obteniendo compartimentos: {str(e)}")
            raise
        
        return compartments
    
    def list_exadata_infrastructures(self, compartments):
        self.logger.info("Iniciando búsqueda de infraestructuras Exadata...")
        
        for compartment_id in compartments:
            try:
                self.logger.debug(f"Buscando Exadata en compartimento: {compartment_id}")
                response = self.database_client.list_cloud_exadata_infrastructures(
                    compartment_id=compartment_id
                )
                
                self.logger.debug(f"Respuesta para compartimento {compartment_id}: {len(response.data)} Exadata encontrados")
                
                for exadata in response.data:
                    self.exadata_info[exadata.display_name] = {
                        'ocid': exadata.id,
                        'compartment_id': compartment_id,
                        'lifecycle_state': exadata.lifecycle_state
                    }
                    self.logger.info(f"Exadata encontrado: {exadata.display_name} - {exadata.id} - Estado: {exadata.lifecycle_state}")
                    
            except Exception as e:
                self.logger.error(f"Error obteniendo Exadata en compartimento {compartment_id}: {str(e)}")
        
        self.logger.info(f"Total de Exadata encontrados: {len(self.exadata_info)}")
        if not self.exadata_info:
            self.logger.warning("¡ADVERTENCIA! No se encontraron infraestructuras Exadata")
    
    def get_date_range(self, days_ahead=30):
        current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        future_date = current_date + timedelta(days=days_ahead)
        
        current_str = current_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        future_str = future_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        self.logger.info(f"Rango de búsqueda: {current_str} hasta {future_str}")
        return current_str, future_str
    
    def convert_to_mexico_time(self, utc_time_str):
        try:
            if utc_time_str.endswith('Z'):
                utc_time_str = utc_time_str[:-1] + '+00:00'
            elif '+' not in utc_time_str and 'Z' not in utc_time_str:
                utc_time_str += '+00:00'
                
            utc_time = datetime.fromisoformat(utc_time_str)
            mexico_time = utc_time - timedelta(hours=6)
            return mexico_time.strftime('%Y-%m-%d %H:%M:%S CST')
        except Exception as e:
            self.logger.error(f"Error convirtiendo tiempo {utc_time_str}: {str(e)}")
            return utc_time_str
    
    def format_patching_time(self, minutes):
        if not minutes:
            return "N/A"
        hours = minutes // 60
        remaining_minutes = minutes % 60
        if hours > 0:
            return f"{hours}h {remaining_minutes}m"
        else:
            return f"{remaining_minutes}m"

    def get_maintenance_info(self, exadata_name, exadata_data):
        maintenance_runs = []
        current_date, future_date = self.get_date_range()
        exadata_ocid = exadata_data['ocid']
        
        self.logger.info(f"=== Buscando mantenimientos para {exadata_name} ===")
        self.logger.info(f"OCID: {exadata_ocid}")
        self.logger.info(f"Rango de fechas: {current_date} hasta {future_date}")
        
        try:
            for compartment_id in self.compartments:
                try:
                    self.logger.debug(f"Buscando mantenimientos en compartimento: {compartment_id}")
                    
                    response_all = self.database_client.list_maintenance_runs(
                        compartment_id=compartment_id
                    )
                    
                    self.logger.debug(f"Total mantenimientos en compartimento {compartment_id}: {len(response_all.data)}")
                    
                    response_filtered = self.database_client.list_maintenance_runs(
                        compartment_id=compartment_id,
                        target_resource_id=exadata_ocid
                    )
                    
                    self.logger.debug(f"Mantenimientos para {exadata_name} en compartimento {compartment_id}: {len(response_filtered.data)}")
                    
                    for run in response_filtered.data:
                        self.logger.info(f"Mantenimiento encontrado:")
                        self.logger.info(f"  - Nombre: {run.display_name}")
                        self.logger.info(f"  - Estado: {run.lifecycle_state}")
                        self.logger.info(f"  - Tipo: {getattr(run, 'maintenance_type', 'N/A')}")
                        self.logger.info(f"  - Subtipo: {getattr(run, 'maintenance_subtype', 'N/A')}")
                        self.logger.info(f"  - Programado: {run.time_scheduled}")
                        self.logger.info(f"  - Target Resource: {getattr(run, 'target_resource_id', 'N/A')}")
                        
                        valid_states = ["SCHEDULED", "IN_PROGRESS", "SUCCEEDED", "SKIPPED", "FAILED"]
                        if run.lifecycle_state not in valid_states:
                            self.logger.info(f"Omitiendo mantenimiento con estado: {run.lifecycle_state}")
                            continue

                        if run.time_scheduled:
                            scheduled_time = run.time_scheduled.isoformat()
                            
                            scheduled_dt = run.time_scheduled.replace(tzinfo=None)
                            current_dt = datetime.fromisoformat(current_date.replace('Z', ''))
                            future_dt = datetime.fromisoformat(future_date.replace('Z', ''))
                            
                            self.logger.debug(f"Comparando fechas:")
                            self.logger.debug(f"  Actual: {current_dt}")
                            self.logger.debug(f"  Programado: {scheduled_dt}")
                            self.logger.debug(f"  Futuro: {future_dt}")
                            self.logger.debug(f"  En rango: {current_dt <= scheduled_dt <= future_dt}")
                            
                            mexico_time = self.convert_to_mexico_time(scheduled_time)
                            
                            patching_time = "N/A"
                            if hasattr(run, 'estimated_patching_time') and run.estimated_patching_time:
                                if hasattr(run.estimated_patching_time, 'total_estimated_patching_time'):
                                    minutes = run.estimated_patching_time.total_estimated_patching_time
                                    patching_time = self.format_patching_time(minutes)
                            
                            maintenance_data = [
                                exadata_name,
                                getattr(run, 'maintenance_subtype', 'N/A') or getattr(run, 'maintenance_type', 'N/A') or "N/A",
                                run.lifecycle_state,
                                scheduled_time,
                                mexico_time,
                                patching_time
                            ]
                            maintenance_runs.append(maintenance_data)
                            self.logger.info(f"✓ Mantenimiento agregado al reporte")
                        else:
                            self.logger.warning(f"Mantenimiento sin fecha programada: {run.display_name}")
                            
                except Exception as e:
                    self.logger.error(f"Error al obtener mantenimiento en compartimento {compartment_id} para {exadata_name}: {str(e)}")
            
            self.logger.info(f"Total de mantenimientos encontrados para {exadata_name}: {len(maintenance_runs)}")
            return maintenance_runs
            
        except Exception as e:
            self.logger.error(f"Error general al obtener mantenimiento para {exadata_name}: {str(e)}")
            return []    

    def generate_report(self):
        self.logger.info("=== INICIANDO GENERACIÓN DE REPORTE ===")
        all_maintenance_runs = []
        
        try:
            compartments = self.list_compartments(self.tenancy_id)
            self.logger.info(f"Compartimentos a revisar: {len(compartments)}")
            
            self.list_exadata_infrastructures(compartments)
            
            if not self.exadata_info:
                self.logger.error("No se encontraron infraestructuras Exadata. Verifica permisos y configuración.")
                with open(self.output_file, 'w') as f:
                    f.write("No se encontraron infraestructuras Exadata.\n")
                    f.write("Verifica permisos y configuración del perfil OCI.\n")
                return
            
            for exadata_name, exadata_data in self.exadata_info.items():
                self.logger.info(f"Procesando mantenimiento para {exadata_name}")
                maintenance_runs = self.get_maintenance_info(exadata_name, exadata_data)
                all_maintenance_runs.extend(maintenance_runs)
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                if not all_maintenance_runs:
                    f.write("No existen mantenimientos programados en los próximos 30 días.\n")
                    f.write(f"Infraestructuras Exadata revisadas: {len(self.exadata_info)}\n")
                    f.write(f"Compartimentos revisados: {len(self.compartments)}\n")
                    self.logger.info("No se encontraron mantenimientos programados")
                else:
                    f.write("EXADATA-NAME,MAINTENANCE-TYPE,STATUS,SCHEDULED-UTC,SCHEDULED-MEXICO,PATCHING-TIME\n")
                    
                    for run in all_maintenance_runs:
                        escaped_run = [str(item).replace('"', '""') for item in run]
                        f.write(','.join([f'"{item}"' for item in escaped_run]) + '\n')
                    
                    self.logger.info(f"Reporte generado exitosamente con {len(all_maintenance_runs)} mantenimientos")
            
            self.logger.info(f"Archivo generado: {self.output_file}")
            print(f"Reporte generado en: {self.output_file}")
            
        except Exception as e:
            self.logger.error(f"Error generando reporte: {str(e)}")
            raise


def main():
    try:
        print("Iniciando Exadata Maintenance Reporter...")
        reporter = ExadataMaintenanceReporter()
        reporter.generate_report()
        print("Proceso completado. Revisa el archivo de log para más detalles.")
    except Exception as e:
        print(f"Error ejecutando el script: {str(e)}")
        raise

if __name__ == "__main__":
    main()