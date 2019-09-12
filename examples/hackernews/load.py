#!/usr/bin/env python3
import sys
import time

import hoply
from hoply.okvs.wiredtiger import WiredTiger
import requests


triplestore = ("subject", "predicate", "object")
triplestore = hoply.open("movielens", prefix=[0], items=triplestore)


if len(sys.argv) == 2:
    maxitem = int(sys.argv[1])
    print("restarting from {}".format(maxitem))
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
        except Exception:
            print("> failed!")
