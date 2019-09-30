#!/usr/bin/env python3
import asyncio
import aiohttp
import base64
import sys
from datetime import datetime
import json
import sys
from async_timeout import timeout


WAYBACK = "http://archive.org/wayback/available"


filename = sys.argv[1]


def eprint(message):
    print(message, file=sys.stderr)


eprint('>>> input: {}'.format(filename))


def iter_urls(session):
    with open(filename) as f:
        for line in f:
            if 'url' not in line:
                continue
            item = json.loads(line)
            yield item


def time2timestamp(integer):
    return datetime.fromtimestamp(integer).strftime("%Y%m%d")


class TimeoutException(Exception):
    pass


async def dump(item, session):
    for _ in range(5):
        try:
            url = item["url"]
        except KeyError:
            pass
        else:
            try:
                # try first to fetch from the wayback machine
                timestamp = time2timestamp(item["time"])
                params = dict(timestamp=timestamp, url=url)
                async with session.get(WAYBACK, params=params) as response:
                    if response.status == 200:
                        response = await response.json()
                    else:
                        response = dict(archived_snapshots=False)
                # check is available
                if (
                    response["archived_snapshots"]
                    and response["archived_snapshots"]["closest"]["available"]
                    and response["archived_snapshots"]["closest"]["status"] == "200"
                ):
                    url = response["archived_snapshots"]["closest"]["url"]
                else:
                    # fallback
                    url = item['url']

                # at last, print on stdout
                async with timeout(2):
                    async with session.get(url, verify_ssl=False) as response:
                        body = await response.read()
                        content_type = response.content_type
                        charset = response.charset
                encoded = base64.b64encode(body)
                encoded = encoded.decode("ascii")
                print('{}\t{}\t{}\t{}'.format(item['url'], content_type, charset, encoded))
            except Exception as exc:
                continue
            else:
                return


COUNT = 10_000


async def crawler(session, generator):
    while True:
        try:
            item = next(generator)
            await dump(item, session)
        except StopAsyncIteration:
            global COUNT
            COUNT -= 1
            return


async def main():
    session = aiohttp.ClientSession()

    generator = iter_urls(filename)

    for _ in range(COUNT):
        asyncio.create_task(crawler(session, generator))

    while True:
        if COUNT == 0:
            await session.close()
            return
        else:
            await asyncio.sleep(1)


asyncio.run(main())
