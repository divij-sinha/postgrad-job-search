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
import logger


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

def launch(url_list, keywords):
    all_job_listings = []
    seen_links = set()  # Set to store seen job links

    for comp in url_list:
        organization_name = comp[0]
        url = comp[1]
        sector = comp[2]
        try:
            # Fetch the page content
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                for element in soup.find_all('a', href=True):
                    job_title = element.text.strip()
                    if any(keyword.lower() in job_title.lower() for keyword in keywords):
                        job_link = element['href']
                        if not job_link.startswith("http"):
                            job_link = url + job_link

                        # deduplicate
                        if job_link not in seen_links:
                            seen_links.add(job_link)  # Mark this job link as seen

                            job_info = {
                                'organization': organization_name,
                                'title': job_title,
                                'apply_link': job_link,
                                'sector': 
                            }
                            all_job_listings.append(job_info)
            else:
                logging.error(f"Failed to fetch {url}: Status code {response.status_code}")
        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")

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

