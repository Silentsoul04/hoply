import base64
import wiredtiger


config = "create,log=(enabled=true,file_max=512MB),cache_size=1024MB"
wt = wiredtiger.wiredtiger_open("wt", config)
session = wt.open_session()
session.create("table:test", "key_format=u,value_format=u")
cursor = session.open_cursor("table:test")

index = 0
with open("out.log") as f:
    session.begin_transaction()
    for line in f:
        if line == "# BEGIN TRANSACTION\n":
            session.commit_transaction()
            session.reset()  # XXX: try to release some memory.
            index += 1
            print("transaction", index)
            session.begin_transaction()
        line = line.strip().encode("ascii")
        key = base64.b64decode(line)
        cursor.set_key(key)
        cursor.set_value(b"\x01")
        cursor.insert()

session.commit_transaction()
