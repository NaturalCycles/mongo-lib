import {
  TEST_TABLE,
  testDao,
  testDB,
  testItems,
  testItemUnsavedSchema,
} from '@naturalcycles/db-dev-lib'
import { CommonDao, DBQuery } from '@naturalcycles/db-lib'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { toArray } from 'rxjs/operators'
import { MongoDB } from '../../mongo.db'

jest.setTimeout(60000)
require('dotenv').config()
const { MONGO_URI } = requireEnvKeys('MONGO_URI')

const mongo = new MongoDB({
  uri: MONGO_URI,
  db: 'db1',
})

afterAll(async () => {
  await mongo.close()
})

const items = testItems(5)

test('test1', async () => {
  await mongo.saveBatch(TEST_TABLE, items)

  const _r = await mongo.getByIds(TEST_TABLE, items.map(i => i.id))
  // console.log(r)
  // const r2 = await mongo.deleteByIds(TEST_TABLE, items.map(i => i.id))
  // console.log(rr)

  const q = new DBQuery(TEST_TABLE).filter('even', '=', true).order('id', true)
  // const r3 = await mongo.runQuery(q)
  // const r3 = await mongo.deleteByQuery(q)
  // console.log(r3)
  // const r4 = await mongo.runQuery(new DBQuery(TEST_TABLE))
  const _r4 = await mongo
    .streamQuery(q)
    .pipe(toArray())
    .toPromise()
  // console.log(r4)
  await mongo.deleteByIds(TEST_TABLE, items.map(i => i.id))
})

test('testDB', async () => {
  // await redis.resetCache()
  await testDB(mongo, DBQuery)
})

test('testDao', async () => {
  const testItemDao = new CommonDao({
    table: TEST_TABLE,
    db: mongo,
    bmUnsavedSchema: testItemUnsavedSchema,
    dbmUnsavedSchema: testItemUnsavedSchema,
  })
  await testDao(testItemDao, DBQuery)
})
