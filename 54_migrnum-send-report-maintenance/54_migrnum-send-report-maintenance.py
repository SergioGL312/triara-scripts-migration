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
    "HCLOPEZ@rednacional.com",
    "ESOTRES@blitzsoftware.com.mx", 
    "NORU@rednoroeste.com", 
    "JCBADILL@rednacional.com",
    "MNLIBERA@serviciosrnx.com",
    "JCVENTUR@blitzsoftware.com.mx", 
    "MPEUSEBI@rednacional.com",
    "EPLEDEZM@serviciosrnx.com",
    "lp.nubeoracle@triara.com", 
    "RN_ST@rednacional.com",
    "ramirezl@rednacional.com",
    "JESERNA@rednacional.com", 
    "RAMIREZL@rednacional.co"
]
BCC_RECIPIENTS = [
    "oracle.cloud@triara.com"
]

now = datetime.now()
start_date = now.strftime('%Y-%m-%d')
start_date_formatted = now.replace(hour=19, minute=0, second=0).strftime('%Y-%m-%d')
end_date = (now + timedelta(days=7)).strftime('%Y-%m-%d')

report_path = os.path.join(os.getcwd(), f"backup-report/reports/rnum/rnum-maintenance-exadata-{start_date}.csv")
os.makedirs(os.path.dirname(report_path), exist_ok=True)

subject_base = f"RNUM - Migración OCI Región 2 - Reporte de mantenimientos al Exadata ({start_date_formatted} a {end_date})"

EMAIL_CONTENTS = {
    "normal": {
        "subject": subject_base,
        "body": (
            "Este reporte se genera como recordatorio de los mantenimientos programados por ORACLE, "
            "los cuales pueden ajustarse manualmente si es necesario.\n\nAdjunto a este correo encontrará el reporte."
        )
    },
    "no_maintenance": {
        "subject": subject_base,
        "body": "No existen mantenimientos programados."
    },
    "error": {
        "subject": f"ERROR - {subject_base}",
        "body": "ERROR: El archivo de mantenimientos no se generó correctamente."
    }
}

def send_email(subject: str, body: str, recipients: list, bcc: list, attachment: str = None):
    msg = MIMEMultipart()
    msg['From'] = SMTP_USER
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body))

    if attachment and os.path.exists(attachment):
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
    if os.path.isfile(report_path):
        with open(report_path, 'r') as f:
            content = f.read()

        if "No existen mantenimientos programados." in content:
            send_email(
                EMAIL_CONTENTS["no_maintenance"]["subject"],
                EMAIL_CONTENTS["no_maintenance"]["body"],
                TO_RECIPIENTS,
                BCC_RECIPIENTS
            )
        else:
            send_email(
                EMAIL_CONTENTS["normal"]["subject"],
                EMAIL_CONTENTS["normal"]["body"],
                TO_RECIPIENTS,
                BCC_RECIPIENTS,
                attachment=report_path
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
