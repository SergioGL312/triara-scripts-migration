import os
import csv
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from collections import defaultdict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER", "sergigl312@gmail.com")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "htjr gewv pyom vtbb")

class PaseMaintenanceMailer:
    def __init__(self):
        now = datetime.now()
        self.start_date = now.replace(hour=19, minute=0, second=0).strftime('%Y-%m-%d')
        self.end_date = (now + timedelta(days=7)).strftime('%Y-%m-%d')
        
        self.updatefile = f"backup-report/reports/pase/pase-maintenance-exadata-{self.start_date}.csv"
        
        self.to = "" # ORACLE.CLOUD@triara.com
        
        self.subject = f"PASE - Reporte de mantenimientos al Exadata ({self.start_date} a {self.end_date})"
        self.body = ("Este reporte se genera como recordatorio de los mantenimientos programados por ORACLE, "
                    "los cuales pueden ajustarse manualmente si es necesario.\n\n\n\n")
        
        self.all_exadata = ["mxqr-oexp01", "mxqr-oexd02"]

        self.exadata_maintenances = defaultdict(str)
        self.exadata_found = {}

    def read_csv_file(self):
        """Lee y procesa el archivo CSV (equivalente al while IFS=, read -r loop)"""
        try:
            with open(self.updatefile, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                
                for row in csv_reader:
                    if len(row) >= 1 and row[0] == "EXADATA-NAME":
                        continue
                    
                    if len(row) >= 5:
                        exadata_name = row[0].strip('"')
                        maintenance_type = row[1].strip('"')
                        scheduled_utc = row[2].strip('"')
                        scheduled_mexico = row[3].strip('"')
                        patching_time = row[4].strip('"')
                        
                        self.exadata_found[exadata_name] = 1
                        
                        if maintenance_type == "No existen mantenimientos programados.":
                            self.exadata_maintenances[exadata_name] = "No existen mantenimientos programados"
                        else:
                            self.exadata_maintenances[exadata_name] += f"{maintenance_type} {scheduled_mexico} hora México\n"
            
            return True
            
        except FileNotFoundError:
            logging.error(f"ERROR: El archivo de mantenimientos no se generó correctamente o no existe: {self.updatefile}")
            return False
        except Exception as e:
            logging.error(f"Error al leer el archivo CSV: {e}")
            return False

    def build_email_body(self):
        """Construye el cuerpo del correo (equivalente a la construcción de email_body en bash)"""
        email_body = "R E S U M E N\n"
        
        for exa in self.all_exadata:
            if exa in self.exadata_maintenances and self.exadata_maintenances[exa]:
                lines = self.exadata_maintenances[exa].strip().split('\n')
                for line in lines:
                    if line.strip():
                        email_body += f"{exa}: {line}\n"
            else:
                email_body += f"{exa}: No existen mantenimientos programados\n"
        
        email_body += "\n\n\n\n**Para más información consulte el archivo adjunto.**"
        
        return email_body

    def send_email(self, email_body):
        """Envía el correo electrónico (equivalente a mailx)"""
        try:
            msg = MIMEMultipart()
            msg['From'] = SMTP_USER
            msg['To'] = self.to
            msg['Subject'] = self.subject
            
            full_body = f"{self.body}\n\n{email_body}"
            msg.attach(MIMEText(full_body, 'plain', 'utf-8'))
            
            if os.path.exists(self.updatefile):
                with open(self.updatefile, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(self.updatefile))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(self.updatefile)}"'
                msg.attach(part)
                logging.info(f"📎 Archivo adjunto: {os.path.basename(self.updatefile)}")
            
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, [self.to], msg.as_string())
            
            logging.info(f"✅ Correo enviado correctamente a: {self.to}")
            logging.info(f"📧 Asunto: {self.subject}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error al enviar correo: {e}")
            return False

    def debug_info(self, email_body):
        """Función de depuración (equivalente a los echo comentados en bash)"""
        print(f"DEBUG - Destinatario: {self.to}")
        print(f"DEBUG - Asunto: {self.subject}")
        print(f"DEBUG - Archivo adjunto: {self.updatefile}")
        print(f"DEBUG - Contenido del mensaje:\n{self.body}\n\n{email_body}")

    def run(self):
        """Función principal que ejecuta todo el proceso"""
        logging.info("🚀 Iniciando proceso PASE - Reporte de mantenimientos Exadata")
        
        if not os.path.isfile(self.updatefile):
            error_msg = f"ERROR: El archivo de mantenimientos no se generó correctamente o no existe: {self.updatefile}"
            print(error_msg)
            logging.error(error_msg)
            return False
        
        if not self.read_csv_file():
            return False

        email_body = self.build_email_body()

        success = self.send_email(email_body)
        
        if success:
            logging.info("✅ Proceso completado exitosamente")
        else:
            logging.error("❌ Proceso completado con errores")
        
        return success

def main():
    """Función principal"""
    try:
        mailer = PaseMaintenanceMailer()
        success = mailer.run()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        logging.info("⏹️ Proceso interrumpido por el usuario")
        exit(1)
    except Exception as e:
        logging.error(f"❌ Error crítico: {e}")
        exit(1)

if __name__ == "__main__":
    main()