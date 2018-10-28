import time
import shlex
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
    start = time.time()
    try:
        run(
            shlex.split("./download.sh {}".format(package)),
            stderr=DEVNULL,
            stdout=DEVNULL,
            timeout=60
        )
    except Exception:
        print('timeout: {}'.format(package))
    else:
        delta = time.time() - start
        print('success: {} @ {:.2f}'.format(package, delta))


with ThreadPoolExecutor(max_workers=10) as e:
    for package in packages:
        e.submit(process, package)
