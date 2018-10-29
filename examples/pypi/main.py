import os
import shlex
from datetime import datetime
from subprocess import run
from subprocess import DEVNULL
try:
    import xmlrpclib
except ImportError:
    import xmlrpc.client as xmlrpclib
from concurrent.futures import ThreadPoolExecutor


client = xmlrpclib.ServerProxy('https://pypi.python.org/pypi')
packages = client.list_packages()

print("total packages: {}".format(len(packages)))


def process(package):
    start = datetime.now()
    print('* {} @ {}'.format(package, start.isoformat()))
    try:
        run(
            shlex.split("./download.sh {}".format(package)),
            stderr=DEVNULL,
            stdout=DEVNULL,
            timeout=20,
        )
    except Exception:
        print('** timeout'.format(package))
    else:
        delta = datetime.now() - start
        print('** success @ {}'.format(package, delta))


with ThreadPoolExecutor(max_workers=10) as e:
    for package in packages:
        filepath = '/home/none/pypi/' + package + '.json'
        if not os.path.exists(filepath):
            e.submit(process, package)
