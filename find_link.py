"""
This script will find all Content body field containing a specific string. This can be used to locate all nodes with
a customized URL that requires an individual update.
"""
from lib import my_env
from lib import tuin_store
from lib.tuin_store import *

cfg = my_env.init_env("tuin_migrate", __file__)
db = cfg['Main']['db']
logging.info("Start application")
tuin = tuin_store.init_session(db=db)

str2find = "snoeitechnieken-en-onderhoud/plantenfamilies"
contents = tuin.query(Content).filter(Content.body.like("%{s2r}%".format(s2r=str2find))).all()
cnt = 0
for content in contents:
    cnt += 1
    print(content.title)
print("Count: {c}".format(c=cnt))
logging.info("End application")
