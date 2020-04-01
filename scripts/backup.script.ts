/*

DEBUG=nc* yarn tsn backup.script.ts

 */

import { dbPipelineBackup } from '@naturalcycles/db-lib'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { runScript } from '@naturalcycles/nodejs-lib/dist/script'
import { MongoDB } from '../src'
import { tmpDir } from '../src/test/paths.cnst'

const { MONGO_URI } = requireEnvKeys('MONGO_URI')

const mongoDB = new MongoDB({
  uri: MONGO_URI,
  db: 'db1',
})

runScript(async () => {
  const limit = 0
  const concurrency = 16

  await dbPipelineBackup({
    db: mongoDB,
    outputDirPath: `${tmpDir}/backup`,
    concurrency,
    limit,
    // errorMode,
  })
})
