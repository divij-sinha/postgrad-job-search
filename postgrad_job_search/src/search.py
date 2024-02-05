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
    idx_list = []
    job_titles = []  

    for idx, url in enumerate(url_list):
        driver.implicitly_wait(2)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
	
	 tags_to_check = [
	    ('h1', 'job-title'),  # Common for main job titles
	    ('h2', 'job-title'),  # Secondary titles or section headers
	    ('h3', 'job-title'),  # Tertiary titles, often used for job listings
	    ('div', 'job-listing'),  # Div containers for job details
	    ('div', 'opening'),  # Another common class for job openings
	    ('a', 'apply-now'),  # Links to application pages
	    ('li', 'position'),  # List items for individual job positions
	    ('span', 'location'),  # Span tags for location details
	    ('p', 'description'),  # Paragraphs for brief job descriptions
	    ('ul', 'listings'),  # Unordered lists containing job listings
	    ('a', 'job-link'),  # Direct links to job descriptions
	    ('div', 'job-description'),  # Div containers for detailed job descriptions]
		 
        for tag, class_name in tags_to_check:
    		for job in soup.find_all(tag, class_=class_name):
	            job_title = job.text.strip()
	            for key in keywords:
	                if key.lower() in job_title.lower():
	                    print(f"Keyword: {key} found in job title: {job_title}")
	                    idx_list.append(idx)
	                    job_titles.append(job_title)
	                    break  # Break if keyword is found to avoid duplicates
	        if not job_titles:  # If no job titles found with the keywords
	            print("No keywords found in job titles!")

    driver.quit()
	
    return idx_list, job_titles


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


def email(notification_df):
    """
    Sends an email notification

    Inputs: notification_df (Pandas DataFrame): DataFrame of companies with current job openings matching the keywords

    Returns: None
    """

    receiver_email = os.environ.get("R_EMAIL_ADDRESS")
    sender_email = os.environ.get("S_EMAIL_ADDRESS")
    password = os.environ.get("EMAIL_PASSWORD")
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    # load and set environment variables and other necessary variables

    msg = MIMEMultipart()
    today = datetime.date.today().strftime("%Y-%m-%d")

    # Set the email parameters
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg["Subject"] = f"New Job Alert — {today}"

    html = f"""
		<html>
			<body>
				<h1>Daily Post Grad Job Search Notification</h1>
				<p>Here are the results for companies with positions open now matching your keywords</p>
				{build_table(notification_df, 'blue_dark')}
			</body>
		</html>
		"""
	
    msg.attach(MIMEText(html, "html"))

    # create the connection and send the email below
    context = ssl.create_default_context()
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.ehlo() 
        server.starttls(context=context)  
        server.ehlo() 
        server.login(sender_email, password)

        server.sendmail(sender_email, receiver_email, msg.as_string())

    except Exception as e:
        # print error
        print(e)


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
