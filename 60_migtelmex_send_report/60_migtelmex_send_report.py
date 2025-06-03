import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "sergigl312@gmail.com"
SMTP_PASSWORD = "htjr gewv pyom vtbb"

TO_RECIPIENTS = [
    # Poner los correos TO_RECIPIENTS
]
BCC_RECIPIENTS = [
    # Poner los correos BCC_RECIPIENTS
]

yesterday = datetime.now() - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d')

oracle_report_path = f"backup-report/reports/telmex/backups-db-oracle-{date_str}.csv"
mysql_report_path = f"backup-report/reports/telmex/backups-db-mysql-{date_str}.csv"

EMAIL_CONTENTS = {
    "normal": {
        "subject": "TELMEX- Migracion OCI Region 2 - Reporte de respaldos disponibles de BDs Oracle y MySQL",
        "body": "Reportes adjuntos."
    },
    "error": {
        "subject": "ERROR - TELMEX - Migracion OCI Region 2 - Reporte de respaldos disponibles de BDs Oracle y MySQL",
        "body": f"No existe el reporte(s):\n{oracle_report_path}\n{mysql_report_path}"
    }
}

def send_email(subject: str, body: str, recipients: list, bcc: list, attachments: list = None):
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body))
    
    if attachments:
        for attachment in attachments:
            if os.path.exists(attachment):
                with open(attachment, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(attachment))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
                msg.attach(part)
    
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipients + bcc, msg.as_string())
        print("✅ Correo enviado correctamente.")
    except Exception as e:
        print(f"❌ Error al enviar correo: {e}")

def main():
    oracle_exists = os.path.isfile(oracle_report_path)
    mysql_exists = os.path.isfile(mysql_report_path)
    
    if oracle_exists and mysql_exists:
        send_email(
            EMAIL_CONTENTS["normal"]["subject"],
            EMAIL_CONTENTS["normal"]["body"],
            TO_RECIPIENTS,
            BCC_RECIPIENTS,
            attachments=[oracle_report_path, mysql_report_path]
        )
    else:
        send_email(
            EMAIL_CONTENTS["error"]["subject"],
            EMAIL_CONTENTS["error"]["body"],
            TO_RECIPIENTS,
            BCC_RECIPIENTS
        )

if __name__ == "__main__":
    main()