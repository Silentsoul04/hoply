#!/usr/bin/env python3
import os
import shlex
import sys
import traceback
from datetime import datetime
from pathlib import Path
from subprocess import run
from subprocess import DEVNULL
from concurrent.futures import ThreadPoolExecutor


def process(package):
    start = datetime.now()
    print('start {} @ {}'.format(package, start.isoformat()))
    try:
        run(
            shlex.split("./download.sh {}".format(package)),
            stderr=DEVNULL,
            stdout=DEVNULL,
            timeout=60,
        )
    except Exception as exc:
        print('timeout {}'.format(package))
        filepath = '/home/pypi/pypi/{}/failed.timestamp'.format(package)
        with Path(filepath).open('w') as f:
            traceback.print_exc(file=f)
    else:
        delta = datetime.now() - start
        print('success {} @ {}'.format(package, delta))


with ThreadPoolExecutor(max_workers=6) as e:
    for package in sys.stdin:
        package = package.strip()
        filepath = '/home/none/pypi/{}/end.timestamp'.format(package)
        if not os.path.exists(filepath):
            e.submit(process, package)
