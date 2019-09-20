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
LOCK_STDOUT = asyncio.Lock()
LOCK_GENERATOR = asyncio.Lock()


async def dump(uid, session):
    url = "https://hacker-news.firebaseio.com/v0/item/{}.json".format(uid)
    async with session.get(url) as response:
        item = await response.json()
    if not item:
        return
    async with LOCK_STDOUT:
        print(json.dumps(item, ensure_ascii=False))


COUNT = 10000


async def crawler(session):
    while True:
        try:
            async with LOCK_GENERATOR:
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
