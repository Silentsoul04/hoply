#!/usr/bin/env python3
import sys
from uuid import uuid4

import hoply
from hoply.okvs.wiredtiger import WiredTiger

import requests
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWebKitWidgets import *


class Render(QWebPage):

    def __init__(self, url):
        self.app = QApplication(sys.argv)
        QWebPage.__init__(self)
        self.loadFinished.connect(self._loadFinished)
        self.mainFrame().load(QUrl(url))
        self.app.exec_()

    def _loadFinished(self, result):
        self.frame = self.mainFrame()
        self.app.quit()


def url2html(url):
    r = Render(url)
    result = r.frame.toHtml()
    return str(result)


def make_uid():
    return uuid4().hex


triplestore = ('subject', 'predicate', 'object')
triplestore = hoply.open('movielens', prefix=[0], items=triplestore)


if sys.argv == 2:
    maxitem = int(sys.argv[1])
else:
    maxitem = requests.get('https://hacker-news.firebaseio.com/v0/maxitem.json').json()


with WiredTiger('wt') as storage:
    for uid in range(maxitem, 0, -1):
        with hoply.transaction(storage) as tr:
            print('{} / {}'.format(uid, maxitem))
            url = 'https://hacker-news.firebaseio.com/v0/item/{}.json'.format(uid)
            item = requests.get(url).json()
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
                url = item['url']
            except KeyError:
                pass
            else:
                # check the page still exists and is not a redirect
                response = requests.head(url)
                if response.status_code == 200:
                    html = url2html(url)
                    triplestore.add(tr, uid, 'url/html', html)
