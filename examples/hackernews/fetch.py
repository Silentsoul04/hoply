#!/usr/bin/env python3
import base64
import json
import requests
import sys
import time

from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import timeout_decorator


options = Options()
options.headless = True
driver = webdriver.Firefox(options=options, executable_path="./geckodriver")


def eprint(message):
    print(message, file=sys.stderr)


class TimeoutException(Exception):
    pass


@timeout_decorator.timeout(60, timeout_exception=TimeoutException)
def url2html(url):
    driver.get(url)
    time.sleep(1)
    out = driver.page_source
    return out


def url2html_with_retry(url):
    for _ in range(5):
        try:
            return url2html(url)
        except TimeoutException:
            continue
    raise TimeoutException()


if len(sys.argv) == 2:
    filename = sys.argv[1]
    start = 0
elif len(sys.argv) == 3:
    filename = sys.argv[1]
    start = int(sys.argv[2])
    eprint("starting with index at {}".format(start))
else:
    eprint("Usage: fetch.py hackernews.jsonl")


WAYBACK = "http://archive.org/wayback/available"


def time2timestamp(integer):
    return datetime.fromtimestamp(integer).strftime("%Y%m%d")


for index, line in enumerate(Path(filename).open()):
    if index < start:
        continue
    if "url" not in line:
        # OPTIM: skip things that have no chance to be a external
        # link, avoid to deserialize with json.loads
        continue
    try:
        item = json.loads(line)
        uid = item["id"]

        try:
            url = item["url"]
        except KeyError:
            eprint("{}: not a story with url: {}".format(index, uid))
        else:
            # always try first to fetch from the wayback machine
            timestamp = time2timestamp(item["time"])
            params = dict(timestamp=timestamp, url=url)
            response = requests.get(WAYBACK, params=params)
            response = response.json()
            # check is available
            if (
                response["archived_snapshots"]
                and response["archived_snapshots"]["closest"]["available"]
                and response["archived_snapshots"]["closest"]["status"] == "200"
            ):
                url = response["archived_snapshots"]["closest"]["url"]
                eprint(
                    "{}: wayback machine works with {} at {}".format(index, uid, url)
                )
            else:
                response = requests.head(url, allow_redirects=True)
                if response.status_code != 200:
                    eprint("{}: skip {} {}".format(index, uid, url))
                    continue
            # good, let's download with selenium
            try:
                html = url2html_with_retry(url)
            except TimeoutException:
                eprint("{}: timeout with {}".format(index, uid))
                url = item["url"]
                eprint("{}: fallback to direct download".format(index))
                response = requests.head(url, allow_redirects=True)
                if response.status_code != 200:
                    eprint("{}: skip {} {}".format(index, uid, url))
                    continue
                html = url2html_with_retry(url)
            encoded = base64.b64encode(html.encode("utf-8"))
            encoded = encoded.decode("ascii")
            print("{}\t{}".format(item["url"], encoded))
    except TimeoutException:
        eprint("{}: give up on {}".format(index, url))
    except Exception:
        eprint("{}: some error".format(index))
