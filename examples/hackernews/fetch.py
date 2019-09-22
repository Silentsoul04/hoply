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


if len(sys.argv) == 2:
    filename = sys.argv[1]
else:
    print("Usage: fetch.py hackernews.jsonl")


WAYBACK = "http://archive.org/wayback/available"


def time2timestamp(integer):
    return datetime.fromtimestamp(integer).strftime("%Y%m%d")


for line in Path(filename).open():
    if "url" not in line:
        continue
    try:
        item = json.loads(line)
        uid = item["id"]
        try:
            url = item["url"]
        except KeyError:
            eprint("not a story with url: {}".format(uid))
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
                eprint("wayback machine works with {} at {}".format(uid, url))
            else:
                response = requests.head(url, allow_redirects=True)
                if response.status_code != 200:
                    eprint("skip {} {}".format(uid, url))
                    continue
            # good, let's download with selenium
            html = url2html(url)
            encoded = base64.b64encode(html.encode("utf-8"))
            encoded = encoded.decode("ascii")
            print("{}\t{}".format(url, encoded))
    except TimeoutException:
        eprint("timeout with {}".format(uid))
    except Exception:
        eprint("some error")
