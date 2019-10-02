#!/usr/bin/env python3
import asyncio
import aiohttp
import sys
import requests
import json
import sys
import string


def eprint(message):
    print(message, file=sys.stderr)


if len(sys.argv) == 3:
    base_url = sys.argv[1]
    apfrom = sys.argv[2]
    eprint("Restarting '{}' from '{}'".format(base_url, apfrom))
elif len(sys.argv) == 2:
    base_url = sys.argv[1]
    apfrom = None
    eprint("Starting '{}' from scratch".format(base_url))
else:
    eprint("Usage: ./dump.py BASE_URL [FROM]")
    sys.exit(1)

if not base_url.endswith("/"):
    base_url += "/"

WIKI_ALL_PAGES = (
    "{}w/api.php?action=query&list=allpages&format=json&aplimit=500&apfrom={}"
)
WIKI_HTML = "{}api/rest_v1/page/html/{}"
WIKI_METADATA = "{}api/rest_v1/page/metadata/{}"


VALID = set(string.punctuation) + set("qwertyuiopasdfghjklzxcvbnm _")


async def iter_titles(session):
    global apfrom
    apfrom_ = apfrom if apfrom is not None else ""
    apfrom = apfrom_
    while True:
        url = WIKI_ALL_PAGES.format(base_url, apfrom)
        async with session.get(url) as response:
            items = await response.json()
            for item in items["query"]["allpages"]:
                title = item["title"]
                surface = set(title) - VALID
                if surface:
                    continue
                else:
                    yield item["title"]
            # continue?
            apfrom = items.get("continue", {}).get("apcontinue")
            if apfrom is None:
                break


async def dump(title, session):
    for _ in range(5):
        try:
            # html
            url = WIKI_HTML.format(base_url, title)
            html = await session.get(url)
            html = await html.text()
            # metadata
            url = WIKI_METADATA.format(base_url, title)
            metadata = await session.get(url)
            metadata = await metadata.json()
            out = dict(title=title, html=html, metadata=metadata)
            print(json.dumps(out))
        except:
            continue


COUNT = 50


async def crawler(lock, session, generator):
    while True:
        try:
            async with lock:
                title = generator.__anext__()
                title = await title
            await dump(title, session)
        except StopAsyncIteration:
            global COUNT
            COUNT -= 1
            return


async def main():
    session = aiohttp.ClientSession()
    lock = asyncio.Lock()
    generator = iter_titles(session)

    for _ in range(COUNT):
        asyncio.create_task(crawler(lock, session, generator))

    while True:
        if COUNT == 0:
            await session.close()
            return
        else:
            await asyncio.sleep(1)


asyncio.run(main())
