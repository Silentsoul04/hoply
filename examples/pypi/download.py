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


def process(root, package):
    start = datetime.now()
    print('start {} @ {}'.format(package, start.isoformat()))
    try:
        run(
            shlex.split("./download.sh {} {}".format(root, package)),
            stderr=DEVNULL,
            stdout=DEVNULL,
            timeout=60,
        )
    except Exception as exc:
        print('failed {}'.format(package))
        filepath = root / package / 'failed.timestamp'
        with filepath.open('w') as f:
            traceback.print_exc(file=f)
    else:
        delta = datetime.now() - start
        print('success {} @ {}'.format(package, delta))


def main():
    root = Path(sys.argv[1]).resolve()
    with ThreadPoolExecutor(max_workers=6) as e:
        for package in sys.stdin:
            package = package.strip()
            filepath = str(root / package / 'end.timestamp')
            if os.path.exists(filepath):
                print('skip {}'.format(package))
            else:
                e.submit(process, root, package)


if __name__ == '__main__':
    main()
