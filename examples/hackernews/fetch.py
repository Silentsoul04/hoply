#!/usr/bin/env python3
import base64
import json
import requests
import sys
import time
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


@timeout_decorator.timeout(5, timeout_exception=TimeoutException)
def url2html(url):
    driver.get(url)
    time.sleep(1)
    out = driver.page_source
    return out


if len(sys.argv) == 2:
    filename = sys.argv[1]
else:
    print("Usage: fetch.py hackernews.jsonl")


for line in Path(filename).open():
    try:
        item = json.loads(line)
        uid = item["id"]
        try:
            url = item["url"]
        except KeyError:
            eprint("not a story with url: {}".format(uid))
        else:
            # check the page still exists and that is not a redirect
            response = requests.head(url)
            if response.status_code == 200:
                html = url2html(url)
                encoded = base64.b64encode(html.encode("utf-8"))
                encoded = encoded.decode('ascii')
                print("{}\t{}".format(url, encoded))
            else:
                eprint("not http status code 200: {}".format(uid))
    except TimeoutException:
        eprint("timeout with {}".format(uid))
    except Exception:
        eprint("some error")
