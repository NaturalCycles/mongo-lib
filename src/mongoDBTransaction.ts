import { DBTransaction } from '@naturalcycles/db-lib'
import { MongoDB } from './mongo.db'

/**
 * https://docs.mongodb.com/manual/core/transactions/
 */
export class MongoDBTransaction extends DBTransaction {
  constructor(public db: MongoDB) {
    super(db)
  }

  async commit(): Promise<void> {
    const client = await this.db.client()

    const session = await client.startSession()

    try {
      await session.withTransaction(async () => {
        for await (const op of this._ops) {
          if (op.type === 'saveBatch') {
            // Important: You must pass the session to the operations
            await this.db.saveBatch(op.table, op.dbms, { ...op.opt, session })
          } else if (op.type === 'deleteByIds') {
            await this.db.deleteByIds(op.table, op.ids, { ...op.opt, session })
          } else {
            throw new Error(`DBOperation not supported: ${op!.type}`)
          }
        }
      })
    } finally {
      await session.endSession()
    }
  }
}
