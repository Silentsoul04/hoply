#!/usr/bin/env python3
import base64
import json
import sys
from pathlib import Path


def main():
    directory = sys.argv[1]
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    for line in sys.stdin:
        # unpack
        pack = base64.b64decode(line)
        pack = pack.decode('utf-8')
        unpack = json.loads(pack)
        # output in package-name.json inside directory passed as
        # argument
        name = unpack['name']
        filepath = directory / (name + '.json')
        with filepath.open('w') as f:
            json.dump(unpack, f, indent=True, sort_keys=True)


if __name__ == '__main__':
    main()
