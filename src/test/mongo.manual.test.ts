import { CommonDao, CommonDaoLogLevel } from '@naturalcycles/db-lib'
import { runCommonDaoTest, runCommonDBTest } from '@naturalcycles/db-lib/dist/testing'
import {
  createTestItemsBM,
  TEST_TABLE,
  testItemBMSchema,
} from '@naturalcycles/db-lib/dist/testing/test.model'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { MongoDB } from '../mongo.db'

jest.setTimeout(60000)
require('dotenv').config()
const { MONGO_URI } = requireEnvKeys('MONGO_URI')

const mongoDB = new MongoDB({
  uri: MONGO_URI,
  db: 'db1',
})

afterAll(async () => {
  await mongoDB.close()
})

describe('runCommonDBTest', () => runCommonDBTest(mongoDB))

describe('runCommonDaoTest', () => runCommonDaoTest(mongoDB))

test.skip('some', async () => {
  const dao = new CommonDao({
    table: TEST_TABLE,
    db: mongoDB,
    bmSchema: testItemBMSchema,
    logStarted: true,
    logLevel: CommonDaoLogLevel.DATA_FULL,
  })

  const items = createTestItemsBM(3)
  await dao.saveBatch(items)

  const r = await dao.query().select([]).runQuery()
  console.log(r)
})
