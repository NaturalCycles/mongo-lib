import {
  BaseCommonDB,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonSchema,
  DBQuery,
  DBTransaction,
  mergeDBOperations,
  ObjectWithId,
  RunQueryResult,
} from '@naturalcycles/db-lib'
import { _Memo, _omit } from '@naturalcycles/js-lib'
import { Debug, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonOptions, FilterQuery, MongoClient, MongoClientOptions } from 'mongodb'
import { Transform } from 'stream'
import { dbQueryToMongoQuery } from './query.util'

export type MongoObject<T> = T & { _id: string }

export interface MongoDBCfg {
  uri: string
  db: string
  options?: MongoClientOptions
}

interface MongoCollectionObject {
  name: string
  type: string
}

export interface MongoDBSaveOptions extends CommonDBSaveOptions, CommonOptions {}
export interface MongoDBOptions extends CommonDBOptions, CommonOptions {}

const log = Debug('nc:mongo-lib')

export class MongoDB extends BaseCommonDB implements CommonDB {
  constructor(public cfg: MongoDBCfg) {
    super()
  }

  @_Memo()
  async client(): Promise<MongoClient> {
    const client = new MongoClient(this.cfg.uri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      ...(this.cfg.options || {}),
    })

    await client.connect()

    log(`connect`)
    return client
  }

  async close(): Promise<void> {
    const client = await this.client()
    await client.close()
    log(`close`)
  }

  async ping(): Promise<void> {
    await this.client()
  }

  protected mapToMongo<ROW extends ObjectWithId>(row: ROW): MongoObject<ROW> {
    const { id, ...m } = { ...row, _id: row.id }
    return m as any
  }

  protected mapFromMongo<ROW extends ObjectWithId>(item: MongoObject<ROW>): ROW {
    const { _id, ...row } = { ...item, id: item._id }
    return row as any
  }

  async getTables(): Promise<string[]> {
    const client = await this.client()
    const colObjects: MongoCollectionObject[] = await client
      .db(this.cfg.db)
      .listCollections(
        {},
        {
          nameOnly: true,
          // authorizedCollections: true,
        },
      )
      .toArray()
    return colObjects.map(c => c.name)
  }

  async getTableSchema<ROW extends ObjectWithId>(table: string): Promise<CommonSchema<ROW>> {
    return {
      table,
      fields: [],
    }
  }

  async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt?: MongoDBSaveOptions,
  ): Promise<void> {
    if (!rows.length) return

    const client = await this.client()
    await client
      .db(this.cfg.db)
      .collection(table)
      .bulkWrite(
        rows.map(r => ({
          replaceOne: {
            filter: {
              _id: r.id,
            },
            replacement: this.mapToMongo(r),
            upsert: true,
          },
        })),
        opt,
      )

    // console.log(res)
  }

  async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    if (!ids.length) return []

    const client = await this.client()
    const items: MongoObject<ROW>[] = await client
      .db(this.cfg.db)
      .collection(table)
      .find({
        _id: {
          $in: ids,
        },
      })
      .toArray()
    return items.map(i => this.mapFromMongo(i))
  }

  async deleteByIds(table: string, ids: string[], opt?: MongoDBOptions): Promise<number> {
    if (!ids.length) return 0

    const client = await this.client()
    const { deletedCount } = await client
      .db(this.cfg.db)
      .collection(table)
      .deleteMany(
        {
          _id: {
            $in: ids,
          },
        },
        opt,
      )

    return deletedCount || 0
  }

  async runQuery<ROW extends ObjectWithId, OUT = ROW>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    const client = await this.client()
    const { query, options } = dbQueryToMongoQuery(q)

    const items: MongoObject<OUT>[] = await client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .toArray()

    let rows = items.map(i => this.mapFromMongo(i as any))

    if (q._selectedFieldNames && !q._selectedFieldNames.includes('id')) {
      // special case
      rows = rows.map(r => _omit(r, ['id']))
    }

    return { rows }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const client = await this.client()
    const { query, options } = dbQueryToMongoQuery(q.select([]))

    const items: MongoObject<{}>[] = await client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .toArray()
    return items.length
  }

  async deleteByQuery(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const client = await this.client()
    const { query } = dbQueryToMongoQuery(q)

    const { deletedCount } = await client.db(this.cfg.db).collection(q.table).deleteMany(query)

    return deletedCount || 0
  }

  streamQuery<ROW extends ObjectWithId, OUT = ROW>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): ReadableTyped<OUT> {
    const { query, options } = dbQueryToMongoQuery(q)

    const transform = new Transform({
      objectMode: true,
      transform: (chunk, _encoding, cb) => {
        cb(null, this.mapFromMongo(chunk))
      },
    })

    void this.client()
      .then(client => {
        client.db(this.cfg.db).collection(q.table).find(query, options).stream().pipe(transform)
      })
      .catch(err => transform.emit('error', err))

    return transform
  }

  async distinct<OUT = any>(
    table: string,
    key: string,
    query: FilterQuery<any> = {},
  ): Promise<OUT[]> {
    const client = await this.client()
    return await client.db(this.cfg.db).collection(table).distinct(key, query)
  }

  /**
   * https://docs.mongodb.com/manual/core/transactions/
   */
  async commitTransaction(tx: DBTransaction, opt?: CommonDBSaveOptions): Promise<void> {
    const client = await this.client()
    const session = await client.startSession()
    const ops = mergeDBOperations(tx.ops)

    try {
      await session.withTransaction(async () => {
        for await (const op of ops) {
          if (op.type === 'saveBatch') {
            // Important: You must pass the session to the operations
            await this.saveBatch(op.table, op.rows, { ...opt, session })
          } else if (op.type === 'deleteByIds') {
            await this.deleteByIds(op.table, op.ids, { ...opt, session })
          } else {
            throw new Error(`DBOperation not supported: ${op!.type}`)
          }
        }
      })
    } finally {
      await session.endSession()
    }
    // todo: is catch/revert needed?
  }
}
