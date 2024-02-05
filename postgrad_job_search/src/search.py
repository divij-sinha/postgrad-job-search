import pandas as pd
import os
import smtplib
import ssl
import datetime
from selenium import webdriver
from pretty_html_table import build_table
from dotenv import load_dotenv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
import time


def data_handle():
    """
    Formats the source data Companies.xlsx

    Inputs: None

    Returns: df (Pandas DataFrame): Formatted pandas dataframe
                     url_list (lst): List of URLs
    """
    df = pd.read_excel("Companies.xlsx")
    url_list = df["URL"].values.tolist()
    df_copy = df.drop(df.columns[[1, 4]], axis=1)

    return df_copy, url_list


def launch(url_list, keywords):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    all_job_listings = []

    # Assuming the organization's name is passed in the URL list
    # as a tuple (organization_name, url)
    for organization_name, url in url_list:
        driver.get(url)
        time.sleep(2)  # Wait for the page to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        for element in soup.find_all('a', href=True):  # Look for all 'a' tags with href attribute
            job_title = element.text.strip()
            if any(keyword.lower() in job_title.lower() for keyword in keywords):
                job_link = element['href']
                if not job_link.startswith("http"):
                    job_link = url + job_link  # Ensure the job link is absolute

                job_info = {
                    'organization': organization_name,
                    'title': job_title,
                    'apply_link': job_link
                }
                all_job_listings.append(job_info)

    driver.quit()
    return all_job_listings


def email(job_listings):
    receiver_email = os.environ.get("R_EMAIL_ADDRESS")
    sender_email = os.environ.get("S_EMAIL_ADDRESS")
    password = os.environ.get("EMAIL_PASSWORD")
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    msg = MIMEMultipart()
    today = datetime.date.today().strftime("%Y-%m-%d")
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = f"New Job Alert — {today}"

    # Convert job_listings to a Pandas DataFrame
    job_df = pd.DataFrame(job_listings)

    # Prepare job listings as a table
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

    # SMTP setup and send email
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Email could not be sent. Error: {e}")


def main():
    """
    Runs the script
    """
    keywords = [
        "Data Analyst",
        "Data Scientist",
        "Associate",
        "Data Engineer",
        "Statistian",
        "Data Journalism",
        "Data Journalist",
        "Survey",
    ]
    df, url_list = data_handle()
    job_listings = launch(url_list, keywords)

    if job_listings:
        email(job_listings)
        print("Email sent with job listings")


main()

