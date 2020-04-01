import { TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { MongoDB } from './mongo.db'

test('test1', async () => {
  const mongo = new MongoDB({
    uri: 'abc',
    db: TEST_TABLE,
  })
  await expect(mongo.client()).rejects.toThrow()
})
