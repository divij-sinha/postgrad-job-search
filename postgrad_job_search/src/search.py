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

    tags_to_check = [
        ('h1', 'job-title'),
        ('h2', 'job-title'),
        ('h3', 'job-title'),
        ('div', 'job-listing'),
        ('div', 'opening'),
        ('a', 'apply-now'),
        ('li', 'position'),
        ('span', 'location'),
        ('p', 'description'),
        ('ul', 'listings'),
        ('a', 'job-link'),
        ('div', 'job-description'),
    ]

    for url in url_list:
        driver.get(url)
        time.sleep(2)  # Wait for the page to load
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        for tag, class_name in tags_to_check:
            for element in soup.find_all(tag, class_=class_name):
                job_info = {'title': '', 'location': '', 'description': '', 'apply_link': ''}
                if tag == 'a' and element.has_attr('href'):
                    job_info['apply_link'] = element['href']
                job_info['title'] = element.text.strip()
                if any(keyword.lower() in job_info['title'].lower() for keyword in keywords):
                    # Attempt to find location and description if present
                    location_element = element.find_next_sibling('span', class_='location')
                    if location_element:
                        job_info['location'] = location_element.text.strip()
                    description_element = element.find_next_sibling('p', class_='description')
                    if description_element:
                        job_info['description'] = description_element.text.strip()
                    
                    all_job_listings.append(job_info)

    driver.quit()
    return all_job_listings

def email_needed(idx_list):
    """
    Checks if an update email is needed or not

    Inputs: idx_list (lst) list of indices

    Returns: boolean 
    """
    if len(idx_list) == 0:
        return False
    return True


def parse_index_lst(idx_list, df):
    """
    Parses the index list to create a dataframe of companies to check in an email

    Inputs: 
    	    idx_list (lst): List of indices
            df (Pandas DataFrame): Original DataFrame of companies and information

    Returns: notification_df (Pandas DataFrame): DataFrame of companies with current job openings matching the keywords
    """
    notification_df = df.iloc[idx_list]

    return notification_df


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

    # Build HTML for job listings
    job_html = ""
    for job in job_listings:
        job_html += f"""
            <h3>{job['title']}</h3>
            <p><strong>Location:</strong> {job.get('location', 'N/A')}</p>
            <p><strong>Description:</strong> {job.get('description', 'N/A')}</p>
            <p><a href="{job['apply_link']}">Apply Now</a></p>
            <hr>
        """

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
    idx_list = launch(url_list, keywords)

    if email_needed(idx_list) is True:
        notification_df = parse_index_lst(idx_list, df)
        print(notification_df)
        email(notification_df)

main()
