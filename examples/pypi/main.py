import json
import os
import shlex
from datetime import datetime
from subprocess import run
from subprocess import DEVNULL
from concurrent.futures import ThreadPoolExecutor

# try:
#     import xmlrpclib
# except ImportError:
#     import xmlrpc.client as xmlrpclib


# client = xmlrpclib.ServerProxy('https://pypi.python.org/pypi')
# packages = client.list_packages()

# print("total packages: {}".format(len(packages)))

with open('pypi.top25k.json') as f:
    packages = f.read()

packages = json.loads(packages)
packages = [x['project'] for x in packages['rows']]


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
    except Exception:
        print('timeout {}'.format(package))
    else:
        delta = datetime.now() - start
        print('success {} @ {}'.format(package, delta))


with ThreadPoolExecutor(max_workers=6) as e:
    for package in packages:
        if package == 0:  # wtf?!
            continue
        filepath = '/home/none/pypi/' + package
        if not os.path.exists(filepath):
            e.submit(process, package)
