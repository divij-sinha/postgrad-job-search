import streamlit as st
from search import search
import pandas as pd
import os

from contextlib import contextmanager, redirect_stdout
from io import StringIO


## https://discuss.streamlit.io/t/cannot-print-the-terminal-output-in-streamlit/6602
@contextmanager
def st_capture(output_func):
    with StringIO() as stdout, redirect_stdout(stdout):
        old_write = stdout.write

        def new_write(string):
            ret = old_write(string)
            output_func(stdout.getvalue())
            return ret

        stdout.write = new_write
        yield


st.set_page_config(layout="wide")

st.markdown("# Job Search Scraper")

st.markdown("## How to use")

steps = """
1. Open the following google sheet [https://docs.google.com/spreadsheets/d/19kdQAZzyb_1xmfqrXNoZ56WYW0orUIHI0PQaP0spZQM/edit#gid=0](https://docs.google.com/spreadsheets/d/19kdQAZzyb_1xmfqrXNoZ56WYW0orUIHI0PQaP0spZQM/edit#gid=0)
1. Go to File > Make a Copy
1. Fill the columns shown in the sheet
    - `Company` should be a name for you to identify the firm you are applying for
    - `URL` should be the link to a page with jobs posted for that company
    - `Keywords` is a list of job titles you are interested in
    - `Exclude` is a list of other related words that you definitely do not want
1. DO NOT CHANGE THE COLUMN NAMES
1. You can add new columns, and change the locations of the columns, but do not change their names
1. Once the sheet is ready, go to File > Share > Publish to Web
1. Change "Entire Document" to the specific sheet 
1. Change "Web page" to "Comma-separated values"
1. Click Publish
1. Copy the link shown and paste below  
"""

st.write(steps)

csv_link = st.text_input("Add link as specified below", os.environ["CONFIGCSV"])
df = pd.read_csv(csv_link)

assert "Company" in df.columns
assert "URL" in df.columns
assert "Keywords" in df.columns
assert "Exclude" in df.columns

job_listings = search(df)

st.dataframe(job_listings, width=900)
