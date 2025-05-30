import os
import csv
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
    "earaujo@rednacional.com", "icabrera@rednacional.com", "vcardoso@rednacional.com",
    "jcbadill@rednacional.com", "gesquive@rednacional.com", "egmata@blitzsoftware.com.mx",
    "ggromero@rednacional.com", "hsalazaj@rednacional.com", "ohvillar@rednacional.com",
    "salinasg@rednacional.com"
]
BCC_RECIPIENTS = ["oracle.cloud@triara.com"]

yesterday = datetime.now() - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d')

oracle_report_path = f"backup-report/reports/rnum/backups-db-oracle-{date_str}.csv"
mysql_report_path = f"backup-report/reports/rnum/backups-db-mysql-{date_str}.csv"

def parse_oracle_report(filepath):
    failed = []
    success = []
    with open(filepath, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if len(row) > 5:
                db_name, status = row[1], row[5]
                if status != "ACTIVE":
                    failed.append(f"{db_name} {status}")
                else:
                    success.append(f"{db_name} {status}")
    return failed, success

def parse_mysql_report(filepath):
    failed = []
    success = []
    with open(filepath, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            if len(row) > 5:
                db_name = row[1].strip('"')
                status = row[5].strip('"')
                if status != "ACTIVE":
                    failed.append(f"{db_name} {status}")
                else:
                    success.append(f"{db_name} {status}")
    return failed, success

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
        oracle_failed, oracle_success = parse_oracle_report(oracle_report_path)
        mysql_failed, mysql_success = parse_mysql_report(mysql_report_path)

        body = "\n"
        if oracle_failed:
            body += "DB(s) Oracle con backup fallido:\n" + "\n".join(oracle_failed) + "\n\n"
        else:
            body += "Backups Oracle han sido exitosos.\n\n"

        if mysql_failed:
            body += "DB(s) MySQL con backup fallido:\n" + "\n".join(mysql_failed) + "\n\n"
        else:
            body += "Backups MySQL han sido exitosos.\n\n"

        body += "--------------------------------------------------------\n"
        body += "**Para más información consulte los reportes adjuntos**.\n\n"

        subject = "RNUM - Migracion OCI Region 2 - Reporte de respaldos disponibles de BDs Oracle y MySQL"
        send_email(subject, body, TO_RECIPIENTS, BCC_RECIPIENTS, [oracle_report_path, mysql_report_path])
    else:
        subject = "ERROR - RNUM - Migracion OCI Region 2 - Reporte de respaldos disponibles de BDs Oracle y MySQL"
        body = f"No existe el reporte(s):\n{oracle_report_path}\n{mysql_report_path}"
        send_email(subject, body, TO_RECIPIENTS, BCC_RECIPIENTS)

if __name__ == "__main__":
    main()
