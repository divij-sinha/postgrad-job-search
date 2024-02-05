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
    job_listings = []

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

        found = False
        for tag, class_name in tags_to_check:
            for element in soup.find_all(tag, class_=class_name):
                # Initialize job info dictionary
                job_info = {'title': '', 'location': '', 'description': '', 'apply_link': ''}
                if tag == 'a' and element.has_attr('href'):
                    job_info['apply_link'] = element['href']
                job_info['title'] = element.text.strip()
                
                # Check if any keyword is in the job title
                if any(keyword.lower() in job_info['title'].lower() for keyword in keywords):
                    found = True
                    # Attempt to find location and description if present
                    # This part needs customization based on the website's structure
                    location_element = element.find_next_sibling('span', class_='location')
                    if location_element:
                        job_info['location'] = location_element.text.strip()
                    description_element = element.find_next_sibling('p', class_='description')
                    if description_element:
                        job_info['description'] = description_element.text.strip()
                    
                    job_listings.append(job_info)
                    break  # Optional: break if you only need one job per tag/class combination

        if not found:
            print(f"No matching jobs found in {url}")

    driver.quit()
    return job_listings

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
