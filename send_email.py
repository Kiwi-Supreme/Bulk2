import os
import aiosmtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

async def send_email(recipient: str, subject: str, message: str, attachment_path: str = None) -> bool:
    try:
        email = EmailMessage()
        email["From"] = os.getenv("EMAIL_SENDER")
        email["To"] = recipient
        email["Subject"] = subject
        email.set_content(message)

        if attachment_path:
            with open(attachment_path, "rb") as f:
                file_data = f.read()
                file_name = os.path.basename(attachment_path)
            email.add_attachment(file_data, maintype="application", subtype="vnd.openxmlformats-officedocument.wordprocessingml.document", filename=file_name)

        print("sending mail")
        await aiosmtplib.send(
            email,
            hostname=os.getenv("EMAIL_HOST"),
            port=int(os.getenv("EMAIL_PORT")),
            start_tls=True,
            username=os.getenv("EMAIL_SENDER"),
            password=os.getenv("EMAIL_PASSWORD"),
        )
        print("mail send")
        return True
    except Exception as e:
        print(f"Failed to send email to {recipient}: {e}")
        return False