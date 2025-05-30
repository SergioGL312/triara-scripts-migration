import os
import csv
from datetime import datetime, timedelta
import subprocess

def send_email(to, subject, body, attachment=None, bcc=None):
    """
    Send an email using the mailx command
    
    :param to: Primary recipient email address
    :param subject: Email subject
    :param body: Email body message
    :param attachment: Path to attachment file (optional)
    :param bcc: BCC email address (optional)
    """
    try:
        # Prepare basic mailx command
        mailx_cmd = ['mailx', '-s', subject]
        
        # Add BCC if provided
        if bcc:
            mailx_cmd.extend(['-b', bcc])
        
        # Add attachment if provided
        if attachment:
            mailx_cmd.extend(['-a', attachment])
        
        # Add recipient
        mailx_cmd.append(to)
        
        # Run the subprocess
        process = subprocess.Popen(mailx_cmd, stdin=subprocess.PIPE, text=True)
        process.communicate(input=body)
        
        return process.returncode == 0
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

def generate_maintenance_report():
    """
    Generate maintenance report CSV
    
    :return: Path to generated report file
    """
    # Get current date and 7 days from now
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Construct full path for the report
    base_dir = os.path.join(os.getcwd(), "backup-report")
    report_filename = f"telmex-maintenance-exadata-{start_date}.csv"
    report_path = os.path.join(base_dir, "reports", "telmex", report_filename)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    # TODO: Replace this with actual maintenance check logic
    # For now, we'll simulate a report generation
    try:
        with open(report_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Maintenance', 'Date', 'Description'])
            
            # Simulate no maintenance scenario
            # writer.writerow(['No existen mantenimientos programados.', '', ''])
            
            # Simulate maintenance exists scenario
            writer.writerow(['Exadata Maintenance', '2024-03-30', 'System update'])
        
        return report_path
    except Exception as e:
        print(f"Error generating report: {e}")
        return None

def main():
    # Configuration
    to_email = "oracle.cloud@triara.com"
    bcc_email = ""  # Add BCC email if needed
    
    # Generate start and end dates
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Generate subject lines
    subject_no_maintenance = f"TELMEX - Migracion OCI Region 2 - Reporte de mantenimientos al Exadata ({start_date} a {end_date})"
    subject_file_error = f"ERROR - TELMEX - Migracion OCI Region 2 - Reporte de mantenimientos al Exadata ({start_date} a {end_date})"
    
    try:
        # Generate maintenance report
        report_path = generate_maintenance_report()
        
        if report_path:
            # Check report content
            with open(report_path, 'r') as f:
                report_content = f.read()
            
            if "No existen mantenimientos programados." in report_content:
                # No maintenance scenario
                send_email(
                    to=to_email, 
                    subject=subject_no_maintenance, 
                    body="No existen mantenimientos programados.",
                    bcc=bcc_email
                )
            else:
                # Maintenance exists scenario
                send_email(
                    to=to_email, 
                    subject=subject_no_maintenance, 
                    body="Reporte adjunto.",
                    attachment=report_path,
                    bcc=bcc_email
                )
        else:
            # Error generating report
            send_email(
                to=to_email, 
                subject=subject_file_error, 
                body="ERROR: El archivo de mantenimientos no se generó correctamente.",
                bcc=bcc_email
            )
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()