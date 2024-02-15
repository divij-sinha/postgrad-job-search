import pandas as pd
from playwright.async_api import async_playwright
import os
import itertools
import asyncio

from dotenv import load_dotenv

load_dotenv()


def filter_job_title(job_title, exclude):
    return not any(keyword.lower() in job_title.lower() for keyword in exclude)


async def get_job_from_page(row, context, keywords, exclude):

    page = await context.new_page()
    print(f"trying {row['Company']}")
    job_infos = []
    try:
        await page.goto(row["URL"], wait_until="networkidle", timeout=20_000)
        await page.wait_for_timeout(5000)
        future_urls = [frame.url for frame in page.frames if frame.url != page.url]
        # inner_html = await page.inner_html("*")
        # soup = BeautifulSoup(inner_html, "html.parser")
        elements = await page.query_selector_all("a[href]")
        for element in elements:
            job_title = await element.text_content()
            if filter_job_title(job_title, exclude) and any(
                keyword.lower() in job_title.lower() for keyword in keywords
            ):
                print(job_title)
                job_link = href = await element.get_attribute("href")
                if not job_link.startswith("http"):
                    if job_link.startswith("/"):
                        job_link = job_link[1:]
                    job_link = "/".join(("https:/", page.url.split("/")[2], job_link))
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


async def search(df):
    keywords = df.loc[:, "Keywords"].dropna().to_list()
    exclude = df.loc[:, "Exclude"].dropna().to_list()
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
