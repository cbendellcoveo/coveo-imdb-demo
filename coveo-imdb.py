import base64
import json
import math
import sys
import pathlib
import zlib

import requests
import toml
import typer
from imdb import IMDb
from bs4 import BeautifulSoup

SCRIPT_NAME = "remote-setup-tools"
CONFIG = toml.load("settings.toml")
CREDENTIALS = toml.load('credentials.toml')

COVEO_PLATFORM = "https://platformdev.cloud.coveo.com"
PUSHAPI_ENDPOINT = "https://apidev.cloud.coveo.com/"

platform_session = requests.Session()
platform_session.headers.update(
    {"Authorization": f"Bearer {CREDENTIALS['token']}"})

app = typer.Typer()


def perform_operation_in_batches(values, operation, batch_size: int = 250):
    for chunk in range(int(math.ceil(len(values)/batch_size))):
        start = chunk * batch_size
        finish = (chunk + 1) * batch_size
        print(f"{start} to {finish} of {len(values)}")
        operation(values[start:finish])


def create_fields(organization_id, fields):
    field_endpoint = f"{COVEO_PLATFORM}/rest/organizations/{organization_id}/indexes/fields/batch/create"
    return platform_session.post(field_endpoint, json=fields)


def encode_document(document):
    document['compressedBinaryData'] = base64.encodebytes(
        zlib.compress(document['data'].encode())).decode()
    document['compressionType'] = 'ZLIB'
    document['fileExtension'] = ".txt"
    del document['data']
    return document


def push_documents(organization_id, source_id, batch):
    file_endpoint = f"{PUSHAPI_ENDPOINT}/push/v1/organizations/{organization_id}/files"
    r = platform_session.post(file_endpoint)
    if not r.ok:
        print("error with file endpoint", r.text)
        return None
    j = r.json()
    upload_uri = j["uploadUri"]
    file_id = j["fileId"]
    headers = j["requiredHeaders"]
    print(
        f"Pushing to {upload_uri} under file id {file_id} with required headers {headers}")
    payload = {"addOrUpdate": batch}
    r = requests.put(upload_uri, json=payload, headers=headers)
    if not r.ok:
        print("error with file endpoint", r.text)
        return None
    pushapi_endpoint = f"{PUSHAPI_ENDPOINT}/push/v1/organizations/{organization_id}/sources/{source_id}/documents/batch?fileId={file_id}"
    r = platform_session.put(pushapi_endpoint)
    return r


@app.command()
def fetch_documents_from_imdb(base_url: str = "https://www.imdb.com", chart_url: str = "/chart/top?ref_=helpms_ih_gi_siteindex"):
    r = requests.get(base_url + chart_url)
    if r.ok:
        soup = BeautifulSoup(r.text, 'html.parser')
        movies = [x.find("a")["href"] for x in list(
            soup.find_all('td', {'class': "titleColumn"}))]
        p = pathlib.Path("data/docs.jsonl")
        ia = IMDb()
        with open(p, "w") as out:
            for i, url in enumerate(movies):
                movie = ia.get_movie(url.split("/")[2][2:])
                j = {
                    "documentId": base_url + url,
                    "plot": movie["plot"],
                    "description": movie["plot"],
                    "rating": movie["rating"],
                    "title": movie["title"],
                    "votes": movie["votes"],
                    "languages": ";".join(movie["languages"]),
                    "year": movie["year"],
                    "genres": ";".join(movie["genres"]),
                    "filetype": ".html",
                    "poster_url": movie["cover url"],
                    "data": ""
                }
                out.write(json.dumps(j) + "\n")
                print(f"Finished {i + 1} of {len(movies)}")
    else:
        print(r.status_code)


@app.command()
def add_fields_from_file(path: str):
    fields = json.load(open(path))

    def add_fields(f):
        r = create_fields(CONFIG["organization_id"], f)
        if r.ok:
            print(f"Added {len(f)}")
        else:
            print(r.text)
    perform_operation_in_batches(fields, add_fields)


@app.command()
def push_documents_from_file(path: str):
    def push_batch(docs):
        r = push_documents(CONFIG["organization_id"],
                           CONFIG["source_id"], docs)
        if r.ok:
            print(f"Pushed {len(docs)} documents.")
        else:
            print(r.text)
    documents = [encode_document(json.loads(line))
                 for line in open(path).readlines()]
    perform_operation_in_batches(documents, push_batch, batch_size=5000)


@app.command()
def test_query():
    df = pd.read_csv("data/test_documents.tsv", sep="\t")
    payload = {
        "q": "@uri=\"*dummyfiles*\""
    }
    organization_id = CONFIG["organization_id"]
    r = platform_session.post(
        f"{ COVEO_PLATFORM }/rest/search/v2?organizationId={organization_id}", json=payload)
    found_count = r.json()["totalCount"]
    expected_count = len(df)
    print(
        f"Expected { expected_count }, found { found_count } matching documents.")


def reverse_proxy_command():
    ports_args = "-R 0.0.0.0:12001:localhost:12001 -R 0.0.0.0:12000:localhost:12000 -R 0.0.0.0:52811:localhost:52811"
    ssh_command = "ssh -N -4" if sys.platform.startswith(
        "win") else "ssh -N -f"
    login = f"ubuntu@{CONFIG['aws_machine_url']}"
    return f"{ssh_command} -i {CREDENTIALS['ssh_key']} {login} {ports_args}"


def main():
    app()


if __name__ == "__main__":
    app()
