from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
import uuid
from search import search
import pandas as pd
import os
from typing import Annotated

from fastapi import FastAPI, Form


app = FastAPI()

templates = Jinja2Templates(directory="templates")


@app.get("/download/{file_name}", response_class=FileResponse)
async def download_file(file_name):
    return FileResponse(f"out/{file_name}.csv")


@app.get("/")
async def start_redirect():
    return RedirectResponse(
        url="/start/home",
    )


@app.get("/start/{code}")
async def start(request: Request, code: str = "home"):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"code": code}
    )


@app.post("/search_page")
def call_search(request: Request, input_link: Annotated[str, Form()]):
    if input_link == "https://secret":
        input_link = os.environ["CONFIGCSV"]
    df = pd.read_csv(input_link)
    print(input_link)
    # try:
    assert "Company" in df.columns
    assert "URL" in df.columns
    assert "Keywords" in df.columns
    assert "Exclude" in df.columns
    job_listings = search(df)
    job_listings = job_listings.sort_values(by=["Company", "Title"])
    file_name = uuid.uuid4()
    file_path = f"out/{file_name}.csv"
    job_listings.to_csv(file_path, index=False)
    job_listings_html = job_listings.to_html(
        index=False,
        render_links=True,
        classes="table table-striped w-25",
        justify="left",
        col_space="100px",
    )
    return templates.TemplateResponse(
        request=request,
        name="result.html",
        context={
            "table_html": job_listings_html,
            "file_name": file_name,
        },
    )

    # except:
    # return RedirectResponse(url="/error", status_code=status.HTTP_303_SEE_OTHER)


# st.write(steps)

#
# df = pd.read_csv(csv_link)

# assert "Company" in df.columns
# assert "URL" in df.columns
# assert "Keywords" in df.columns
# assert "Exclude" in df.columns

# job_listings = search(df)

# st.dataframe(job_listings, width=900)
