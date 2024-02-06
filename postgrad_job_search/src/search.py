import pandas as pd
import os
import smtplib
import ssl
import datetime
import requests
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
import logging
import json
from selenium import webdriver

load_dotenv()

def filter_job_title(job_title):
    exclude_keywords = ['internship', 'student']
    return not any(keyword.lower() in job_title.lower() for keyword in exclude_keywords)

def launch(keywords, json_file='job_listings.json'):
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vSQC3Io4qy97NqcUbSjMjcaC08A4GASQeK8CyQnqodXQhkEVb7m4ve-ofsdO_Frz6RAZPCBzeVrXV4r/pub?output=csv")
    
    stored_jobs = load_job_listings(json_file)
    stored_links = {job['apply_link'] for job in stored_jobs}
    new_job_listings = []

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  
    driver = webdriver.Chrome(options=options)

    for idx, row in df.iterrows():
        url = row['URL']
        organization_name = row['Company']
        sector = row['Sector']
        print(f"going to this url: {url}")
        # time.sleep(30)
        driver.implicitly_wait(2)
        driver.get(url)
        try:
            print(url)
            driver.implicitly_wait(2)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            print(soup.text)
            for element in soup.find_all('a', href=True):
                job_title = element.text.strip()
                if filter_job_title(job_title) and any(keyword.lower() in job_title for keyword in keywords):
                    job_link = element['href']
                    if not job_link.startswith("http"):
                        job_link = url + job_link

                    if job_link not in stored_links:
                        job_info = {
                            'organization': organization_name,
                            'title': job_title,
                            'apply_link': job_link,
                            'sector': sector,
                            'is_new': 'NEW'  # Mark as new
                        }
                        new_job_listings.append(job_info)
                        stored_jobs.append(job_info)
        except Exception as e:
            print(f"Error scraping {url}: {e}")
    
    driver.quit()
    save_job_listings(json_file, stored_jobs)
    
    return new_job_listings

def email(job_listings):
    receiver_email = os.getenv("R_EMAIL_ADDRESS")
    sender_email = os.getenv("S_EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")
    
    if not (receiver_email and sender_email and password):
        logging.error("Email credentials are not set in environment variables.")
        return

    new_job_listings = [job for job in job_listings if job.get('is_new') == 'NEW']

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
        df['title'] = df.apply(lambda x: f'<a href="{x["apply_link"]}">{x["title"]}</a>', axis=1)
        df.drop('apply_link', axis=1, inplace=True)
        df.rename(columns={'organization': 'Organization', 'title': 'Position', 'sector': 'Sector', 'is_new': 'Status'}, inplace=True)
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

def load_job_listings(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return []

def save_job_listings(file_path, job_listings):
    with open(file_path, 'w') as file:
        json.dump(job_listings, file, indent=4)

def main():
    keywords = [
        "data analyst",
        "data scientist",
        "statistician",
        "research analyst",
        "research associate",
        "policy analyst",
        "data engineer",
        "product manager",
        "product analyst",
        "project manager",
    ]

    job_listings = launch(keywords)
    if job_listings:
        email(job_listings)
        logging.info("Email sent with job listings.")
    else:
        logging.info("No job listings found.")

if __name__ == "__main__":
    main()


