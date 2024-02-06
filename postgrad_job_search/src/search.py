import pandas as pd
import os
import smtplib
import ssl
import datetime
import requests
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from pretty_html_table import build_table
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
import logging
import json

load_dotenv() 


def data_handle():
    """
    Formats the source data from Companies.xlsx
    """
    try:
        df = pd.read_excel("Companies.xlsx")
        url_list = [[row['Name'], row['URL'], row['Sector']] for index, row in df.iterrows()]
        return url_list
    except Exception as e:
        logging.error(f"Error reading Companies.xlsx: {e}")
        return []

def launch(url_list, keywords, json_file='job_listings.json'):
    stored_jobs = load_job_listings(json_file)
    stored_links = {job['apply_link'] for job in stored_jobs}  # Set for fast lookup
    new_job_listings = []

    for comp in url_list:
        organization_name = comp[0]
        url = comp[1]
        sector = comp[2]
        try:
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                for element in soup.find_all('a', href=True):
                    job_title = element.text.strip()
                    if any(keyword.lower() in job_title.lower() for keyword in keywords):
                        job_link = element['href']
                        if not job_link.startswith("http"):
                            job_link = url + job_link

                        if job_link not in stored_links:
                            job_info = {
                                'organization': organization_name,
                                'title': job_title,
                                'apply_link': job_link,
                                'sector': sector,
                                'is_new': True  # Mark as new
                            }
                            new_job_listings.append(job_info)
                            stored_jobs.append(job_info)
        except Exception as e:
            print(f"Error scraping {url}: {e}")

    # Save updated job list to JSON, combining stored and new listings
    save_job_listings(json_file, stored_jobs)

    return new_job_listings

def email(job_listings):
    receiver_email = os.getenv("R_EMAIL_ADDRESS")
    sender_email = os.getenv("S_EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")
    
    if not (receiver_email and sender_email and password):
        logging.error("Email credentials are not set in environment variables.")
        return

    # Filter new job listings
    new_job_listings = [job for job in job_listings if job.get('is_new')]

    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = MIMEMultipart()
    today = datetime.date.today().strftime("%Y-%m-%d")
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = f"New Job Alert — {today}"

    # Prepare HTML content for new job listings
    if new_job_listings:
        new_job_df = pd.DataFrame(new_job_listings)
        new_job_html = build_table(new_job_df, "blue_dark")
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


def load_job_listings(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return []

# Function to save job listings to a JSON file
def save_job_listings(file_path, job_listings):
    with open(file_path, 'w') as file:
        json.dump(job_listings, file, indent=4)

def main():
    keywords = [
        "Data Analyst",
        "Data Scientist",
        "Statistician",
        "Research Analyst",
        "Research",
        "Policy",
        "Data Science",
        "Data"
    ]
    url_list = data_handle()
    if not url_list:
        logging.error("No URLs found. Exiting.")
        return

    job_listings = launch(url_list, keywords)
    if job_listings:
        email(job_listings)
        logging.info("Email sent with job listings.")
    else:
        logging.info("No job listings found.")

if __name__ == "__main__":
    main()

main()

