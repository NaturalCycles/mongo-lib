import { runCommonDaoTest, runCommonDBTest } from '@naturalcycles/db-lib/dist/testing'
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

describe('runCommonDBTest', () => runCommonDBTest(mongoDB))

describe('runCommonDaoTest', () => runCommonDaoTest(mongoDB))
