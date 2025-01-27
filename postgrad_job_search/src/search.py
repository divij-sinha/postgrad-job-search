import pandas as pd
from playwright.async_api import async_playwright
import os
import itertools
import asyncio

from dotenv import load_dotenv

load_dotenv()

default_keywords = [
    "Data Analyst",
    "Data Scientist",
    "Statistician",
    "Research Analyst",
    "Research Associate",
    "Policy Analyst",
    "Data Engineer",
    "Researcher	",
    "Research Scientist",
    "Research Engineer",
    "Data Policy",
    "Statistics",
    "Engineer",
    "Director of Government Client Services",
    "Local Food Coordinator",
]

def match(job_title, l):
    for keyword in l:
        if keyword.lower() in job_title:
            return True
    return False


def clean_link(job_link, page_url):
    if not job_link.startswith("http"):
        if job_link.startswith("/"):
            job_link = job_link[1:]
        job_link = "/".join(("https:/", page_url.split("/")[2], job_link))
    return job_link


async def get_job_from_page(row, context, keywords, exclude):

    page = await context.new_page()
    job_infos = []
    try:
        await page.goto(row["URL"], wait_until="networkidle", timeout=20_000)
        await page.wait_for_timeout(5000)
        future_urls = [frame.url for frame in page.frames if frame.url != page.url]
        elements = await page.query_selector_all("a[href]")
        for element in elements:
            job_title = await element.text_content()
            jll = job_title.lower()
            if match(jll, keywords) and not match(jll, exclude):
                job_link = await element.get_attribute("href")
                job_link = clean_link(job_link, page.url)
                job_info = {
                    "Company": row["Company"],
                    "Title": job_title,
                    "Apply Link": job_link,
                    # "is_new": "NEW",  # Mark as new
                }
                job_infos.append(job_info)
        return {"Company": row["Company"], "URL": future_urls}, job_infos
    except:
        print(f"failed {row['Company']}")
        return {"Company": row["Company"], "URL": [row["URL"]]}, job_infos


async def get_job_listings(df, keywords, exclude):
    all_parts = []
    async with async_playwright() as p:
        if os.environ["PWBROWSER"] == "webkit":
            browser = await p.webkit.launch(headless=True, timeout=100_000)
        elif os.environ["PWBROWSER"] == "chromium":
            browser = await p.chromium.launch(headless=True, timeout=100_000)
        for i, row in df.iterrows():
            context = await browser.new_context()
            all_parts.append(get_job_from_page(row, context, keywords, exclude))
        res = await asyncio.gather(*all_parts)
    try:
        future_urls, job_listings = zip(*res)
        return future_urls, job_listings
    except:
        return None


async def stream_table(ws, full_job_listings):
    if ws is not None:
        full_job_listings = itertools.chain.from_iterable(full_job_listings)
        full_job_listings = pd.DataFrame(full_job_listings).drop_duplicates()
        job_listings_html = full_job_listings.to_html(
            index=False,
            render_links=True,
            classes="table table-striped w-25",
            justify="left",
            col_space="100px",
        )
        await ws.send_json({"show": "results_partial"})
        await ws.send_json({"update_table": job_listings_html})


async def stream_keywords(ws):
    if ws is not None:
        await ws.send_json(
            {"message": "No keywords found, switching to default keywords!"}
        )
        await ws.send_json({"message": ", ".join(default_keywords)})


async def search(df, ws=None):
    try:
        keywords = df.loc[:, "Keywords"].dropna().str.lower().to_list()
    except:
        keywords = default_keywords
    try:
        exclude = df.loc[:, "Exclude"].dropna().str.lower().to_list()
    except:
        exclude = []
    df = df.loc[:, ["Company", "URL"]].drop_duplicates(subset=["Company", "URL"])

    N_PER_RUN = int(os.environ["N_PER_RUN"])
    full_job_listings = []
    links_visited = []
    while df.shape[0] > 0:
        print("run")
        full_future_urls = []
        n = df.shape[0]
        n_runs = (n // N_PER_RUN) + 1
        for i in range(n_runs):
            res = await get_job_listings(
                df.iloc[i * N_PER_RUN : min((i + 1) * N_PER_RUN, n)], keywords, exclude
            )
            if res is not None:
                future_urls, job_listings = res
                full_future_urls.extend(future_urls)
                full_job_listings.extend(job_listings)
                await stream_table(ws, full_job_listings)

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
    return full_job_listings


if __name__ == "__main__":
    df = pd.read_csv(os.environ["CONFIGCSV"])
    jl = asyncio.run(search(df))
    print(jl)
