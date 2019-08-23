import {
  BaseDBEntity,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
} from '@naturalcycles/db-lib'
import { Debug, streamToObservable } from '@naturalcycles/nodejs-lib'
import { MongoClient, MongoClientOptions } from 'mongodb'
import { Observable } from 'rxjs'
import { map } from 'rxjs/operators'
import { dbQueryToMongoQuery } from './query.util'

export type MongoObject<T> = T & { _id: string }

export interface MongoDBCfg {
  uri: string
  db: string
  options?: MongoClientOptions
}

const log = Debug('nc:mongo-lib')

export class MongoDB implements CommonDB {
  constructor (public cfg: MongoDBCfg) {}

  public client!: MongoClient

  async connect (): Promise<void> {
    const client = new MongoClient(this.cfg.uri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      ...(this.cfg.options || {}),
    })

    this.client = await client.connect()

    log(`connect`)
  }

  async close (): Promise<void> {
    await this.client.close()
    log(`close`)
  }

  async resetCache (): Promise<void> {}

  protected mapToMongo<DBM extends BaseDBEntity> (dbm: DBM): MongoObject<DBM> {
    const { id, ...m } = { ...dbm, _id: dbm.id }
    return m as any
  }

  protected mapFromMongo<DBM extends BaseDBEntity> (item: MongoObject<DBM>): DBM {
    const { _id, ...dbm } = { ...item, id: item._id }
    return dbm as any
  }

  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {
    const col = this.client.db(this.cfg.db).collection(table)

    await col.bulkWrite(
      dbms.map(dbm => ({
        replaceOne: {
          filter: {
            _id: dbm.id,
          },
          replacement: this.mapToMongo(dbm),
          upsert: true,
        },
      })),
    )

    // console.log(res)
  }

  async getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    const col = this.client.db(this.cfg.db).collection(table)

    const items: MongoObject<DBM>[] = await col
      .find({
        _id: {
          $in: ids,
        },
      })
      .toArray()
    return items.map(i => this.mapFromMongo(i))
  }

  async deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    const { deletedCount } = await this.client
      .db(this.cfg.db)
      .collection(table)
      .deleteMany({
        _id: {
          $in: ids,
        },
      })

    return deletedCount || 0
  }

  async runQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    const { query, options } = dbQueryToMongoQuery(q)

    const items: MongoObject<DBM>[] = await this.client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .toArray()
    return items.map(i => this.mapFromMongo(i))
  }

  async runQueryCount<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const { query, options } = dbQueryToMongoQuery(q.select([]))

    const items: MongoObject<{}>[] = await this.client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .toArray()
    return items.length
  }

  async deleteByQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const { query } = dbQueryToMongoQuery(q)

    const { deletedCount } = await this.client
      .db(this.cfg.db)
      .collection(q.table)
      .deleteMany(query)

    return deletedCount || 0
  }

  streamQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    const { query, options } = dbQueryToMongoQuery(q)

    return streamToObservable<MongoObject<DBM>>(this.client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .stream() as NodeJS.ReadableStream).pipe(map(i => this.mapFromMongo(i)))
  }
}
