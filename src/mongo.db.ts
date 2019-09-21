import {
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
  ObjectWithId,
  RunQueryResult,
  SavedDBEntity,
} from '@naturalcycles/db-lib'
import { memo } from '@naturalcycles/js-lib'
import { Debug, streamToObservable } from '@naturalcycles/nodejs-lib'
import { FilterQuery, MongoClient, MongoClientOptions } from 'mongodb'
import { Observable, Subject } from 'rxjs'
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
  constructor(public cfg: MongoDBCfg) {}

  @memo()
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

  async resetCache(): Promise<void> {}

  protected mapToMongo<DBM extends ObjectWithId>(dbm: DBM): MongoObject<DBM> {
    const { id, ...m } = { ...dbm, _id: dbm.id }
    return m as any
  }

  protected mapFromMongo<DBM extends SavedDBEntity>(item: MongoObject<DBM>): DBM {
    const { _id, ...dbm } = { ...item, id: item._id }
    return dbm as any
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {
    if (!dbms.length) return

    const client = await this.client()
    await client
      .db(this.cfg.db)
      .collection(table)
      .bulkWrite(
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

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    if (!ids.length) return []

    const client = await this.client()
    const items: MongoObject<DBM>[] = await client
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

  async deleteByIds(table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    if (!ids.length) return 0

    const client = await this.client()
    const { deletedCount } = await client
      .db(this.cfg.db)
      .collection(table)
      .deleteMany({
        _id: {
          $in: ids,
        },
      })

    return deletedCount || 0
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opts?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    const client = await this.client()
    const { query, options } = dbQueryToMongoQuery(q)

    const items: MongoObject<OUT>[] = await client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .toArray()
    return { records: items.map(i => this.mapFromMongo(i as any)) }
  }

  async runQueryCount(q: DBQuery, opts?: CommonDBOptions): Promise<number> {
    const client = await this.client()
    const { query, options } = dbQueryToMongoQuery(q.select([]))

    const items: MongoObject<{}>[] = await client
      .db(this.cfg.db)
      .collection(q.table)
      .find(query, options)
      .toArray()
    return items.length
  }

  async deleteByQuery(q: DBQuery, opts?: CommonDBOptions): Promise<number> {
    const client = await this.client()
    const { query } = dbQueryToMongoQuery(q)

    const { deletedCount } = await client
      .db(this.cfg.db)
      .collection(q.table)
      .deleteMany(query)

    return deletedCount || 0
  }

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opts?: CommonDBOptions,
  ): Observable<OUT> {
    const { query, options } = dbQueryToMongoQuery(q)

    const subj = new Subject<OUT>()

    void this.client()
      .then(client => {
        streamToObservable<MongoObject<OUT>>(client
          .db(this.cfg.db)
          .collection(q.table)
          .find(query, options)
          .stream() as NodeJS.ReadableStream)
          .pipe(map(i => this.mapFromMongo(i as any)))
          .subscribe(subj)
      })
      .catch(err => subj.error(err))

    return subj
  }

  async distinct<OUT = any>(
    table: string,
    key: string,
    query: FilterQuery<any> = {},
  ): Promise<OUT[]> {
    const client = await this.client()
    return await client
      .db(this.cfg.db)
      .collection(table)
      .distinct(key, query)
  }
}
