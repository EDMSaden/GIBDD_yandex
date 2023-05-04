from aiogram import Bot, Dispatcher
import _sqlite3 as sq
import os

# Bot and dispatcher initialization
bot = Bot(os.environ.get('TOKEN'))
dp = Dispatcher(bot)

with sq.connect('GIBDD.db') as con:
        cur = con.cursor()
        cur.execute(f'SELECT label FROM label')
        label = cur.fetchall()
        label = label[0][0]

count_row_examination_paper = 20

with sq.connect('GIBDD.db') as con:
        cur = con.cursor()
        cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name LIKE"examination_paper%"')
        result = cur.fetchall()
        examination_paper = [i[0] for i in result]
        examination_paper.sort()
        print(examination_paper)


import os
import ydb
import ydb.iam

# Create driver in global space.
driver = ydb.Driver(
  endpoint=os.getenv('YDB_ENDPOINT'),
  database=os.getenv('YDB_DATABASE'),
  credentials=ydb.iam.MetadataUrlCredentials())


# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)

session = driver.table_client.session().create()
