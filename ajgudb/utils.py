# AjuDB - wiredtiger powered graph database
# Copyright (C) 2015 Amirouche Boubekki <amirouche@hypermove.net>
# from msgpack import loads
# from msgpack import dumps
from json import loads, dumps

def pack(value):
    return dumps(value)


def unpack(value):
    return loads(value)


class AjguDBException(Exception):
    pass
