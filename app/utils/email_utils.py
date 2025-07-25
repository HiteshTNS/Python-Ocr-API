import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env file

SENDER_EMAIL = os.getenv("SENDER_EMAIL")
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD")
RECEIVER_EMAILS = os.getenv("RECEIVER_EMAILS", "").split(",")
CC_EMAILS = os.getenv("CC_EMAILS", "").split(",")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
def send_mail_notification(html_body: str,file_id:str):
    message = MIMEMultipart()
    message["From"] = SENDER_EMAIL
    message["To"] = ", ".join(RECEIVER_EMAILS)
    message["Cc"] = ", ".join(CC_EMAILS)
    message["Subject"] = f"Pdf Extracted Text Result of file_id - {file_id}"

    message.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
            smtp.sendmail(SENDER_EMAIL, RECEIVER_EMAILS + CC_EMAILS, message.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")
