#!/usr/bin/env python3
import asyncio
import aiohttp
import sys
import requests
import json


if len(sys.argv) == 2:
    maxitem = int(sys.argv[1])
    print("restarting from {}".format(maxitem))
else:
    maxitem = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()


GENERATOR = iter(range(maxitem))


async def dump(uid, session):
    for _ in range(5):
        try:
            url = "https://hacker-news.firebaseio.com/v0/item/{}.json".format(uid)
            async with session.get(url) as response:
                item = await response.json()
            if not item:
                return
            print(json.dumps(item, ensure_ascii=False))
        except asyncio.TimeoutError:
            continue
        else:
            return


COUNT = 5000


async def crawler(session):
    while True:
        try:
            uid = next(GENERATOR)
            await dump(uid, session)
        except StopIteration:
            global COUNT
            COUNT -= 1
            return


async def main():
    session = aiohttp.ClientSession()

    for _ in range(COUNT):
        asyncio.create_task(crawler(session))

    while True:
        if COUNT == 0:
            session.close()
            return
        else:
            await asyncio.sleep(1)


asyncio.run(main())
