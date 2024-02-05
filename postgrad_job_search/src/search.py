import pandas as pd
import os
import smtplib
import ssl
import datetime
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from pretty_html_table import build_table
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()  # Load environment variables

def data_handle():
    """
    Formats the source data from Companies.xlsx
    """
    try:
        df = pd.read_excel("Companies.xlsx")
        url_list = [(row['Name'], row['URL']) for index, row in df.iterrows()]
        return url_list
    except Exception as e:
        logging.error(f"Error reading Companies.xlsx: {e}")
        return []

def launch(url_list, keywords):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    try:
        driver = webdriver.Chrome(options=options)
    except WebDriverException as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return []

    all_job_listings = []

    for organization_name, url in url_list:
        try:
            driver.get(url)
            time.sleep(2)  # Wait for the page to load
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            for element in soup.find_all('a', href=True):
                job_title = element.text.strip()
                if any(keyword.lower() in job_title.lower() for keyword in keywords):
                    job_link = element['href']
                    if not job_link.startswith("http"):
                        job_link = url + job_link

                    job_info = {
                        'organization': organization_name,
                        'title': job_title,
                        'apply_link': job_link
                    }
                    all_job_listings.append(job_info)
        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")

    driver.quit()
    return all_job_listings

def email(job_listings):
    receiver_email = os.getenv("R_EMAIL_ADDRESS")
    sender_email = os.getenv("S_EMAIL_ADDRESS")
    password = os.getenv("EMAIL_PASSWORD")
    
    if not (receiver_email and sender_email and password):
        logging.error("Email credentials are not set in environment variables.")
        return

    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = MIMEMultipart()
    today = datetime.date.today().strftime("%Y-%m-%d")
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = f"New Job Alert — {today}"

    job_df = pd.DataFrame(job_listings)
    job_html = build_table(job_df, "blue_dark")

    html = f"""
    <html>
        <body>
            <h1>Daily Job Search Notification</h1>
            <p>Here are the jobs matching your keywords:</p>
            {job_html}
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

def main():
    keywords = [
        "Data Analyst",
        "Data Scientist",
        "Associate",
        "Data Engineer",
        "Statistician",
        "Data Journalism",
        "Data Journalist",
        "Survey",
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

