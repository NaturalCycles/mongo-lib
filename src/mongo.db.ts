import { Transform } from 'stream'
import { AnyObjectWithId } from '@naturalcycles/db-lib/src/db.model'
import {
  BaseCommonDB,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
  DBTransaction,
  mergeDBOperations,
  ObjectWithId,
  RunQueryResult,
} from '@naturalcycles/db-lib'
import { _Memo, _omit } from '@naturalcycles/js-lib'
import { Debug, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommandOperationOptions, Filter, MongoClient, MongoClientOptions } from 'mongodb'
import { dbQueryToMongoQuery } from './query.util'

export type MongoObject<T> = T & { _id: string }

export interface MongoDBCfg {
  uri: string
  db: string
  options?: MongoClientOptions
}

export interface MongoDBSaveOptions<ROW extends ObjectWithId = AnyObjectWithId>
  extends CommonDBSaveOptions<ROW>,
    CommandOperationOptions {}
export interface MongoDBOptions extends CommonDBOptions, CommandOperationOptions {}

const log = Debug('nc:mongo-lib')

export class MongoDB extends BaseCommonDB implements CommonDB {
  constructor(public cfg: MongoDBCfg) {
    super()
  }

  @_Memo()
  async client(): Promise<MongoClient> {
    const client = new MongoClient(this.cfg.uri, {
      // useNewUrlParser: true,
      // useUnifiedTopology: true,
      ...this.cfg.options,
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

  override async ping(): Promise<void> {
    await this.client()
  }

  protected mapToMongo<ROW extends ObjectWithId>(row: ROW): MongoObject<ROW> {
    const { id: _, ...m } = { ...row, _id: row.id }
    return m as any
  }

  protected mapFromMongo<ROW extends ObjectWithId>(item: MongoObject<ROW>): ROW {
    const { _id, ...row } = { ...item, id: item._id }
    return row as any
  }

  override async getTables(): Promise<string[]> {
    const client = await this.client()
    const colObjects = await client
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

  override async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt: MongoDBSaveOptions<ROW> = {},
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

  override async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    _opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    if (!ids.length) return []

    const client = await this.client()
    const items = (await client
      .db(this.cfg.db)
      .collection(table)
      .find({
        _id: {
          $in: ids,
        },
      })
      .toArray()) as MongoObject<ROW>[]
    return items.map(i => this.mapFromMongo(i))
  }

  override async deleteByIds(
    table: string,
    ids: string[],
    opt: MongoDBOptions = {},
  ): Promise<number> {
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

  override async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<RunQueryResult<ROW>> {
    const client = await this.client()
    const { query, options } = dbQueryToMongoQuery(q)

    const items = (await client
      .db(this.cfg.db)
      .collection<ROW>(q.table)
      .find(query, options) // eslint-disable-line unicorn/no-array-method-this-argument
      .toArray()) as MongoObject<ROW>[]

    let rows = items.map(i => this.mapFromMongo(i as any))

    if (q._selectedFieldNames && !q._selectedFieldNames.includes('id')) {
      // special case
      rows = rows.map(r => _omit(r, ['id']))
    }

    return { rows }
  }

  override async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const client = await this.client()
    const { query, options } = dbQueryToMongoQuery(q.select([]))

    const items: MongoObject<any>[] = await client
      .db(this.cfg.db)
      .collection<ROW>(q.table)
      .find(query, options) // eslint-disable-line unicorn/no-array-method-this-argument
      .toArray()
    return items.length
  }

  override async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const client = await this.client()
    const { query } = dbQueryToMongoQuery(q)

    const { deletedCount } = await client.db(this.cfg.db).collection<ROW>(q.table).deleteMany(query)

    return deletedCount || 0
  }

  override streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): ReadableTyped<ROW> {
    const { query, options } = dbQueryToMongoQuery(q)

    const transform = new Transform({
      objectMode: true,
      transform: (chunk, _encoding, cb) => {
        cb(null, this.mapFromMongo(chunk))
      },
    })

    void this.client()
      .then(client => {
        client
          .db(this.cfg.db)
          .collection<ROW>(q.table)
          .find(query, options) // eslint-disable-line unicorn/no-array-method-this-argument
          .stream()
          .pipe(transform)
      })
      .catch(err => transform.emit('error', err))

    return transform
  }

  async distinct<ROW = any>(table: string, key: string, query: Filter<ROW> = {}): Promise<ROW[]> {
    const client = await this.client()
    return await client.db(this.cfg.db).collection<ROW>(table).distinct(key, query)
  }

  /**
   * https://docs.mongodb.com/manual/core/transactions/
   */
  override async commitTransaction(tx: DBTransaction, opt?: CommonDBSaveOptions): Promise<void> {
    const client = await this.client()
    const session = client.startSession()
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
            throw new Error(`DBOperation not supported: ${(op as any).type}`)
          }
        }
      })
    } finally {
      await session.endSession() // eslint-disable-line @typescript-eslint/await-thenable
    }
    // todo: is catch/revert needed?
  }
}
