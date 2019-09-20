import { runCommonDaoTest, runCommonDBTest } from '@naturalcycles/db-lib'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { MongoDB } from '../../mongo.db'

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

test('testDB', async () => {
  await runCommonDBTest(mongoDB)
})

test('testDao', async () => {
  await runCommonDaoTest(mongoDB)
})
