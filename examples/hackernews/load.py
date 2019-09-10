#!/usr/bin/env python3
import sys
import time
from uuid import uuid4

import hoply
from hoply.okvs.wiredtiger import WiredTiger

import requests

from selenium import webdriver
from selenium.webdriver.firefox.options import Options


options = Options()
options.headless = True
driver = webdriver.Firefox(options=options, executable_path="./geckodriver")


def url2html(url):
    driver.get(url)
    time.sleep(1)
    out = driver.page_source
    return out


def make_uid():
    return uuid4().hex


triplestore = ("subject", "predicate", "object")
triplestore = hoply.open("movielens", prefix=[0], items=triplestore)


if sys.argv == 2:
    maxitem = int(sys.argv[1])
    print('restarting from {}'.format(maxitem))
else:
    maxitem = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()


with WiredTiger("wt") as storage:
    for uid in range(maxitem, 0, -1):
        try:
            with hoply.transaction(storage) as tr:
                print("{} / {}".format(uid, maxitem))
                url = "https://hacker-news.firebaseio.com/v0/item/{}.json".format(uid)
                item = requests.get(url).json()
                if not item:
                    continue
                # add item to database
                for key, value in item.items():
                    if isinstance(value, list):
                        for element in value:
                            triplestore.add(tr, uid, key, element)
                    elif isinstance(value, dict):
                        raise NotImplementedError()
                    else:
                        triplestore.add(tr, uid, key, value)
                # if the item has a url key, fetch the url using qt webkit
                # because most recent webpages rely on in-browser
                # rendering we need a real browser to fetch the content of
                # the page.
                try:
                    url = item["url"]
                except KeyError:
                    pass
                else:
                    # check the page still exists and is not a redirect
                    response = requests.head(url)
                    if response.status_code == 200:
                        html = url2html(url)
                        triplestore.add(tr, uid, "url/html", html)
        except Exception:
            print('> failed!')
