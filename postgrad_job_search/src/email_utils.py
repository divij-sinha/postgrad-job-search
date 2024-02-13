from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
import smtplib
import ssl
import datetime
import pandas as pd

from dotenv import load_dotenv

load_dotenv()


def email(job_listings):
    receiver_email = os.getenv("R_EMAIL_ADDRESS")
    sender_email = os.getenv("S_EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")

    if not (receiver_email and sender_email and password):
        logging.error("Email credentials are not set in environment variables.")
        return

    new_job_listings = [job for job in job_listings if job.get("is_new") == "NEW"]

    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = MIMEMultipart()
    today = datetime.date.today().strftime("%Y-%m-%d")
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = f"New Job Alert — {today}"

    # Convert job listings to DataFrame and adjust for email
    if new_job_listings:
        df = pd.DataFrame(new_job_listings)
        df["title"] = df.apply(
            lambda x: f'<a href="{x["apply_link"]}">{x["title"]}</a>', axis=1
        )
        df.drop("apply_link", axis=1, inplace=True)
        df.rename(
            columns={
                "organization": "Organization",
                "title": "Position",
                "sector": "Sector",
                "is_new": "Status",
            },
            inplace=True,
        )
        new_job_html = df.to_html(escape=False, index=False)
        new_jobs_section = f"<h2>New Job Listings</h2>{new_job_html}"
    else:
        new_jobs_section = "<h2>No New Job Listings Found</h2>"

    html = f"""
    <html>
        <body>
            <h1>Daily Job Search Notification - {today}</h1>
            {new_jobs_section}
        </body>
    </html>
    """

    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=ssl.create_default_context())
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Email could not be sent. Error: {e}")
