import os
import json
import subprocess
from datetime import datetime, timedelta
import pytz
import csv
from typing import List, Dict, Optional

class ExadataMaintenanceReporter:
    """Clase principal para generar reportes de mantenimiento de Exadata"""
    
    def __init__(self, base_dir: str = "backup-report", profile: str = "nubeprivadaoracle"):
        """Inicializa el reporter con configuraciones básicas"""
        self.base_dir = base_dir
        self.profile = profile
        self.utc_tz = pytz.utc
        self.mexico_tz = pytz.timezone('America/Mexico_City')
        
        # Configurar rutas
        self.reports_dir = os.path.join(self.base_dir, "reports", "pase")
        self.scripts_log_dir = os.path.join(self.base_dir, "scripts", "log")
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.scripts_log_dir, exist_ok=True)
        
        # Archivos de salida
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.output_file = os.path.join(self.reports_dir, f"pase-maintenance-exadata-{current_date}.csv")
        self.log_file = os.path.join(self.scripts_log_dir, "pase-maintenance-exadata.log")
        
        # Fechas para el filtro
        self.current_utc = datetime.now(self.utc_tz).replace(hour=19, minute=0, second=0, microsecond=0)
        self.future_utc = self.current_utc + timedelta(days=7)

    def log_message(self, message: str) -> None:
        """Escribe un mensaje en el log"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, 'a') as f:
            f.write(f"[{timestamp}] {message}\n")
        print(message)

    def run_oci_command(self, command_args: List[str]) -> Optional[Dict]:
        """Ejecuta un comando de OCI CLI y devuelve el resultado como JSON"""
        try:
            result = subprocess.run(
                ['oci'] + command_args + ['--profile', self.profile],
                check=True,
                capture_output=True,
                text=True
            )
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            self.log_message(f"Error en comando OCI: {e.stderr}")
        except json.JSONDecodeError:
            self.log_message("Error al decodificar la respuesta JSON")
        return None

    def get_all_compartments(self, parent_ocid: str = None) -> List[Dict]:
        """Recupera recursivamente todos los compartments"""
        compartments = []
        command_args = ['iam', 'compartment', 'list', '--all']
        
        if parent_ocid:
            command_args.extend(['--compartment-id', parent_ocid])
        
        result = self.run_oci_command(command_args)
        if result and 'data' in result:
            for compartment in result['data']:
                if compartment['lifecycle-state'] == 'ACTIVE':
                    compartments.append(compartment)
                    compartments.extend(self.get_all_compartments(compartment['id']))
        
        return compartments

    def get_exadata_in_compartment(self, compartment_id: str) -> List[Dict]:
        """Obtiene todas las Exadatas en un compartment"""
        exadatas = []
        command_args = [
            'db', 'exadata-infrastructure', 'list',
            '--compartment-id', compartment_id,
            '--all'
        ]
        
        result = self.run_oci_command(command_args)
        if result and 'data' in result:
            for exadata in result['data']:
                if exadata['lifecycle-state'] == 'AVAILABLE':
                    exadatas.append({
                        'id': exadata['id'],
                        'name': exadata['display-name'],
                        'compartment_id': compartment_id
                    })
        
        return exadatas

    def get_maintenance_runs(self, exadata_ocid: str, compartment_id: str) -> List[Dict]:
        """Obtiene los maintenance runs para una Exadata"""
        command_args = [
            'db', 'maintenance-run', 'list',
            '--compartment-id', compartment_id,
            '--target-resource-id', exadata_ocid,
            '--all'
        ]
        
        result = self.run_oci_command(command_args)
        return result['data'] if result and 'data' in result else []

    @staticmethod
    def format_patching_time(minutes: int) -> str:
        """Formatea el tiempo de parcheo en horas y minutos"""
        if not minutes:
            return "N/A"
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours}h {mins}m"

    def convert_utc_to_mexico(self, utc_time_str: str) -> str:
        """Convierte tiempo UTC a hora de México"""
        try:
            utc_time = datetime.strptime(utc_time_str, '%Y-%m-%dT%H:%M:%S%z')
            mexico_time = utc_time.astimezone(self.mexico_tz)
            return mexico_time.strftime('%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return utc_time_str

    def filter_maintenance_runs(self, runs: List[Dict]) -> List[Dict]:
        """Filtra los maintenance runs por el rango de fechas"""
        filtered = []
        for run in runs:
            try:
                time_scheduled = datetime.strptime(run['time-scheduled'], '%Y-%m-%dT%H:%M:%S%z')
                if self.current_utc <= time_scheduled <= self.future_utc:
                    filtered.append(run)
            except (KeyError, ValueError):
                continue
        return filtered

    def generate_report(self) -> None:
        """Genera el reporte CSV con los mantenimientos encontrados"""
        found_maintenance = False
        
        with open(self.output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'EXADATA-NAME',
                'MAINTENANCE-TYPE',
                'SCHEDULED-UTC',
                'SCHEDULED-MEXICO',
                'PATCHING-TIME'
            ])
            
            # Obtener todas las Exadatas
            root_compartment_id = os.environ.get('OCI_TENANCY_OCID')
            if not root_compartment_id:
                self.log_message("Error: No se encontró OCI_TENANCY_OCID en las variables de entorno")
                return
            
            all_compartments = self.get_all_compartments(root_compartment_id)
            self.log_message(f"Encontrados {len(all_compartments)} compartments")
            
            all_exadatas = []
            for compartment in all_compartments:
                exadatas = self.get_exadata_in_compartment(compartment['id'])
                all_exadatas.extend(exadatas)
            
            self.log_message(f"Encontradas {len(all_exadatas)} Exadatas")
            
            # Para procesar cada Exadata
            for exadata in all_exadatas:
                exadata_name = exadata['name']
                exadata_ocid = exadata['id']
                compartment_id = exadata['compartment_id']
                
                self.log_message(f"Procesando Exadata: {exadata_name} ({exadata_ocid})")
                
                maintenance_runs = self.get_maintenance_runs(exadata_ocid, compartment_id)
                filtered_runs = self.filter_maintenance_runs(maintenance_runs)
                
                if filtered_runs:
                    for run in filtered_runs:
                        writer.writerow([
                            exadata_name,
                            run.get('maintenance-subtype', 'N/A'),
                            run.get('time-scheduled', 'N/A'),
                            self.convert_utc_to_mexico(run.get('time-scheduled', 'N/A')),
                            self.format_patching_time(
                                run.get('estimated-patching-time', {}).get('total-estimated-patching-time')
                            )
                        ])
                    found_maintenance = True
                else:
                    writer.writerow([exadata_name, "No existen mantenimientos programados.", "", "", ""])
            
            if not found_maintenance:
                writer.writerow(["", "No existen mantenimientos programados para ninguna Exadata.", "", "", ""])

    def run(self) -> None:
        """Método principal que ejecuta todo el proceso"""
        self.log_message("Iniciando script de reporte de mantenimiento de Exadata")
        self.log_message(f"Fecha actual (UTC): {self.current_utc}")
        self.log_message(f"Fecha futura (UTC): {self.future_utc}")
        
        self.generate_report()
        
        self.log_message(f"Reporte generado en: {self.output_file}")
        self.log_message("Script completado")


if __name__ == "__main__":
    reporter = ExadataMaintenanceReporter()
    reporter.run()