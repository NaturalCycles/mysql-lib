import {
  BaseDBEntity,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
  RunQueryResult,
  SavedDBEntity,
} from '@naturalcycles/db-lib'
import { filterUndefinedValues, logMethod, memo } from '@naturalcycles/js-lib'
import { Debug, streamToObservable } from '@naturalcycles/nodejs-lib'
import { Pool, PoolConfig, PoolConnection, TypeCast } from 'mysql'
import * as mysql from 'mysql'
import { Observable, Subject } from 'rxjs'
import { map } from 'rxjs/operators'
import { Readable, Transform } from 'stream'
import { promisify } from 'util'
import { dbQueryToSQLDelete, dbQueryToSQLSelect, insertSQL } from './query.util'

export interface MysqlDBOptions extends CommonDBOptions {}
export interface MysqlDBSaveOptions extends CommonDBSaveOptions {}

/**
 * @default false / undefined
 */
export interface MysqlDBCfg extends PoolConfig {
  /**
   * If true - will log all produced SQL queries.
   */
  logSQL?: boolean

  /**
   * If true - will emit logs of connection events.
   */
  debugConnections?: boolean
}

const log = Debug('nc:mysql-lib')

const typeCast: TypeCast = (field, next) => {
  // cast TINY and BIT to boolean
  if (['TINY', 'BIT'].includes(field.type) && field.length === 1) {
    return field.string() === '1' // 1 = true, 0 = false
  }

  return next()
}

export class MysqlDB implements CommonDB {
  constructor(cfg: MysqlDBCfg) {
    this.cfg = {
      typeCast,
      ...cfg,
    }
  }

  cfg!: MysqlDBCfg

  init(): void {
    this.pool()
  }

  async resetCache(table?: string): Promise<void> {}

  @memo()
  @logMethod({ logResult: false })
  pool(): Pool {
    const pool = mysql.createPool(this.cfg)
    if (this.cfg.debugConnections) {
      pool.on('acquire', con => {
        log(`acquire ${con.threadId}`)
      })

      pool.on('connection', con => {
        log(`connection ${con.threadId}`)
      })

      pool.on('enqueue', () => {
        log(`enqueue`)
      })

      pool.on('release', con => {
        log(`release ${con.threadId}`)
      })
    }

    pool.on('error', err => {
      log.error(`error`, err)
    })

    return pool
  }

  async close(): Promise<void> {
    const pool = this.pool()
    await promisify(pool.end.bind(pool))
  }

  async getConnection(): Promise<PoolConnection> {
    const pool = this.pool()
    return promisify(pool.getConnection.bind(pool))()
  }

  // GET
  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt?: MysqlDBOptions,
  ): Promise<DBM[]> {
    if (!ids.length) return []
    const q = new DBQuery<DBM>(table).filterEq('id', ids)
    const { records } = await this.runQuery(q, opt)
    return records
  }

  // QUERY
  async runQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opt?: MysqlDBOptions,
  ): Promise<RunQueryResult<DBM>> {
    const sql = dbQueryToSQLSelect(q)
    const records = await this.runSQL<DBM[]>(sql)
    return { records: records.map(r => filterUndefinedValues(r, true)) }
  }

  async runSQL<RESULT>(sql: string, opt?: MysqlDBOptions): Promise<RESULT> {
    if (this.cfg.logSQL) log(sql)
    return new Promise(async (resolve, reject) => {
      const con = await this.getConnection()
      con.query(sql, (err, res) => {
        con.release()
        if (err) return reject(err)
        resolve(res)
      })
    })
  }

  async runQueryCount<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const { records } = await this.runQuery(q.select(['count(*) as _count']))
    return (records[0] as any)._count
  }

  streamQuery<DBM extends SavedDBEntity>(q: DBQuery<DBM>, opt?: CommonDBOptions): Observable<DBM> {
    const subj = new Subject<DBM>()

    const sql = dbQueryToSQLSelect(q)
    if (this.cfg.logSQL) log(sql)
    this.streamSQL(sql)
      .then(stream => {
        // pipe stream into previously created Subject
        streamToObservable<DBM>(stream)
          .pipe(map(dbm => filterUndefinedValues(dbm, true)))
          .subscribe(subj)
      })
      .catch(err => {
        subj.error(err)
      })

    return subj
  }

  private async streamSQL(sql: string): Promise<Readable> {
    return new Promise<Readable>(async (resolve, reject) => {
      const con = await this.getConnection()
      const terminate = (err: Error) => {
        con.release()
        return reject(err)
      }

      const s = con
        .query(sql)
        .on('error', terminate)
        .on('finish', () => con.release())
        .on('fields', _fields => {
          con.pause()
          const stream = s
            .stream()
            .pipe(
              new Transform({
                objectMode: true,
                transform: (rows: any, encoding, callback) => {
                  callback(undefined, rows)
                },
              }),
            )
            .on('pause', () => con.pause())
            .on('resume', () => con.resume())
          resolve(stream)
        })
    })
  }

  // SAVE
  async saveBatch<DBM extends BaseDBEntity>(
    table: string,
    dbms: DBM[],
    opt?: MysqlDBSaveOptions,
  ): Promise<void> {
    if (!dbms.length) return
    const sql = insertSQL(table, dbms)
    await this.runSQL(sql)
  }

  // DELETE
  /**
   * Limitation: always returns [], regardless of which rows are actually deleted
   */
  async deleteByIds(table: string, ids: string[], opt?: MysqlDBOptions): Promise<number> {
    if (!ids.length) return 0
    const sql = dbQueryToSQLDelete(new DBQuery(table).filterEq('id', ids))
    const res = await this.runSQL<any>(sql)
    return res.affectedRows
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const sql = dbQueryToSQLDelete(q)
    const res = await this.runSQL<any>(sql)
    return res.affectedRows
  }
}
