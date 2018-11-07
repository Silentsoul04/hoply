#!/usr/bin/env python3
import base64
import json
import sys
from pathlib import Path


def main():
    directory = sys.argv[1]
    directory = Path(directory).resolve()
    for timestamp in directory.glob('*/end.timestamp'):
        package = timestamp.parent
        out = dict()
        out['name'] = package.name
        # pack dependencies
        with (package / 'dependencies.json').open() as f:
            dependencies = json.load(f)
        out['pipenv json-tree'] = dependencies
        # pack metadata
        with (package / 'metadata.json').open() as f:
            metadata = json.load(f)
        out['pypi metadata'] = metadata
        # python version
        with (package / 'Pipfile.lock').open() as f:
            lock = json.load(f)
        python_version = lock['_meta']['requires']['python_version']
        out['python version'] = python_version
        # serialize into json and base64 encode it
        pack = json.dumps(out)
        pack = pack.encode('ascii')
        pack = base64.b64encode(pack)
        pack = pack.decode('ascii')
        # et voila !
        sys.stdout.write(pack)
        sys.stdout.write('\n')


if __name__ == '__main__':
    main()
