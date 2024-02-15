import pandas as pd
from bs4 import BeautifulSoup
import json
from playwright.sync_api import sync_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
import os
import itertools
import time
import asyncio

from dotenv import load_dotenv

load_dotenv()
N_PER_RUN = int(os.environ["N_PER_RUN"])

keywords_ = [
    "Data Analyst",
    "Data Scientist",
    "Statistician",
    "Research Analyst",
    "Research Associate",
    "Policy Analyst",
    "Data Engineer",
    "Researcher",
    "Research Scientist",
    "Research Engineer",
    "Data Policy",
    "Statistics",
    "Engineer",
    "Director of Government Client Services",
]


def filter_job_title(job_title):
    return not any(keyword.lower() in job_title.lower() for keyword in exclude)


def get_job_from_page(row, i):
    with sync_playwright() as p:
        webkit = p.webkit.launch(headless=True, timeout=100_000)
        page = webkit.new_page()
        print(f"trying {row['Company']}")
        job_infos = []
        try:
            page.goto(row["URL"], wait_until="networkidle", timeout=20_000)
            print(f"loaded {row['Company']}")
            future_urls = [
                frame.url for frame in page.frames if frame.url != row["URL"]
            ]
            inner_html = page.inner_html("*")
            soup = BeautifulSoup(inner_html, "html.parser")
            for element in soup.find_all("a", href=True):
                job_title = element.text.strip()
                if filter_job_title(job_title) and any(
                    keyword.lower() in job_title.lower() for keyword in keywords
                ):
                    job_link = element["href"]
                    if not job_link.startswith("http"):
                        job_link = "".join((row["URL"], job_link))
                    job_info = {
                        "company": row["Company"],
                        "title": job_title,
                        "apply_link": job_link,
                        # "is_new": "NEW",  # Mark as new
                    }
                    job_infos.append(job_info)
            return {"Company": row["Company"], "URL": future_urls}, job_infos
        except:
            print(f"failed {row['Company']}")
            return {"Company": row["Company"], "URL": [row["URL"]]}, job_infos


def get_job_listings(df):
    all_parts = []
    for i, row in df.iterrows():
        all_parts.append(get_job_from_page(row, i))
    # res = await asyncio.gather(*all_parts)
    future_urls, job_listings = zip(*all_parts)

    return future_urls, job_listings


def search(df):
    global keywords, exclude
    import time

    start = time.time()
    keywords = df.loc[:, "Keywords"].dropna().to_list()
    exclude = df.loc[:, "Exclude"].dropna().to_list()
    df = df.loc[:, ["Company", "URL"]].drop_duplicates(subset=["Company", "URL"])

    full_job_listings = []
    links_visited = []
    while df.shape[0] > 0:
        full_future_urls = []
        n = df.shape[0]
        n_runs = (n // N_PER_RUN) + 1
        for i in range(n_runs):
            res = get_job_listings(df.iloc[i * N_PER_RUN : min((i + 1) * N_PER_RUN, n)])
            future_urls, job_listings = res
            full_future_urls.extend(future_urls)
            full_job_listings.extend(job_listings)

        future_df = pd.DataFrame(full_future_urls)
        future_df = future_df.explode("URL")
        future_df = future_df.reset_index(drop=True)
        future_df = future_df.dropna()
        future_df = future_df.loc[future_df.URL != "about:blank"]
        future_df = future_df.loc[~future_df.URL.isin(links_visited)]
        future_df = future_df.loc[~future_df.URL.str.contains("recaptcha")]
        future_df = future_df.loc[~future_df.URL.str.contains("paypal")]
        future_df = future_df.loc[~future_df.URL.str.contains("stripe")]
        future_df = future_df.drop_duplicates()
        links_visited.extend(future_df.URL)
        df = future_df

    full_job_listings = itertools.chain.from_iterable(full_job_listings)
    full_job_listings = pd.DataFrame(full_job_listings).drop_duplicates()
    dur = time.time() - start
    print(f"{dur=}")
    return full_job_listings


if __name__ == "__main__":
    df = pd.read_csv(os.environ["CONFIGCSV"])
    jl = search(df)
    print(jl)
