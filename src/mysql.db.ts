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
import { Debug, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { Connection, Pool, PoolConfig, PoolConnection, TypeCast } from 'mysql'
import * as mysql from 'mysql'
import { Transform } from 'stream'
import { promisify } from 'util'
import { dbQueryToSQLDelete, dbQueryToSQLSelect, insertSQL } from './query.util'

export interface MysqlDBOptions extends CommonDBOptions {
  /**
   * If passed - will use that connection.
   */
  con?: Connection
}
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

  async init(): Promise<void> {
    this.pool()
    const con = await this.getConnection()
    con.release()
  }

  async resetCache(table?: string): Promise<void> {}

  @memo()
  @logMethod({ logResult: false })
  pool(): Pool {
    const pool = mysql.createPool(this.cfg)

    if (this.cfg.debugConnections) {
      pool.on('acquire', con => {
        log(`acquire(${con.threadId})`)
      })

      pool.on('connection', con => {
        log(`connection(${con.threadId})`)
      })

      pool.on('enqueue', () => {
        log(`enqueue`)
      })

      pool.on('release', con => {
        log(`release(${con.threadId})`)
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

  /**
   * Be careful to always call `con.release()` when you get connection with this method.
   */
  async getConnection(): Promise<PoolConnection> {
    const pool = this.pool()
    return promisify(pool.getConnection.bind(pool))()
  }

  /**
   * Manually create a single (not pool) connection.
   * Be careful to manage this connection yourself (open, release, etc).
   */
  async createSingleConnection(): Promise<Connection> {
    const con = mysql.createConnection(this.cfg)
    await promisify(con.connect.bind(con))()
    const { threadId } = con

    if (this.cfg.debugConnections) {
      log(`createSingleConnection(${threadId})`)

      con.on('connect', () => log(`createSingleConnection(${threadId}).connect`))
      con.on('drain', () => log(`createSingleConnection(${threadId}).drain`))
      con.on('enqueue', () => log(`createSingleConnection(${threadId}).enqueue`))
      con.on('end', () => log(`createSingleConnection(${threadId}).end`))
    }

    con.on('error', err => {
      log.error(`createSingleConnection(${threadId}).error`, err)
    })

    return con
  }

  // GET
  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt: MysqlDBOptions = {},
  ): Promise<DBM[]> {
    if (!ids.length) return []
    const q = new DBQuery<DBM>(table).filterEq('id', ids)
    const { records } = await this.runQuery(q, opt)
    return records
  }

  // QUERY
  async runQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opt: MysqlDBOptions = {},
  ): Promise<RunQueryResult<DBM>> {
    const sql = dbQueryToSQLSelect(q)
    const records = await this.runSQL<DBM[]>(sql)
    return { records: records.map(r => filterUndefinedValues(r, true)) }
  }

  async runSQL<RESULT>(sql: string, opt: MysqlDBOptions = {}): Promise<RESULT> {
    if (this.cfg.logSQL) log(sql)

    return new Promise(async (resolve, reject) => {
      // const con = opt.con || (await this.getConnection())
      ;(opt.con ? opt.con : this.pool()).query(sql, (err, res) => {
        // if ((con as PoolConnection).release) {
        //   ;(con as PoolConnection).release()
        // }
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

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<DBM>,
    opt: MysqlDBOptions = {},
  ): ReadableTyped<OUT> {
    const sql = dbQueryToSQLSelect(q)

    if (this.cfg.logSQL) log(`stream: ${sql}`)

    // return this.streamSQL(sql, opt)
    return this.pool()
      .query(sql)
      .stream()
      .pipe(
        new Transform({
          objectMode: true,
          transform(dbm, _encoding, cb) {
            cb(null, filterUndefinedValues(dbm, true))
          },
        }),
      )
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
