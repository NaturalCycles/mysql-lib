import {
  BaseDBEntity,
  CommonDB,
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonSchema,
  DBQuery,
  RunQueryResult,
  SavedDBEntity,
} from '@naturalcycles/db-lib'
import { filterUndefinedValues, memo, _mapKeys, _mapValues } from '@naturalcycles/js-lib'
import { Debug, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { white } from '@naturalcycles/nodejs-lib/dist/colors'
import { Connection, Pool, PoolConfig, PoolConnection, QueryOptions, TypeCast } from 'mysql'
import * as mysql from 'mysql'
import { Transform } from 'stream'
import { promisify } from 'util'
import { dbQueryToSQLDelete, dbQueryToSQLSelect, insertSQL } from './query.util'
import {
  commonSchemaToMySQLDDL,
  mapNameFromMySQL,
  MySQLTableStats,
  mysqlTableStatsToCommonSchemaField,
} from './schema/mysql.schema.util'

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

const BOOLEAN_TYPES = new Set(['TINY', 'TINYINT', 'INT'])
const BOOLEAN_BIT_TYPES = new Set(['BIT'])

const typeCast: TypeCast = (field, next) => {
  // cast to boolean
  if (field.length === 1) {
    if (BOOLEAN_BIT_TYPES.has(field.type)) {
      const b = field.buffer()
      if (b === null || b === undefined) return undefined // null = undefined
      return b[0] === 1 // 1 = true, 0 = false
    }

    if (BOOLEAN_TYPES.has(field.type)) {
      const s = field.string()
      // console.log(field.name, field.type, s, s?.charCodeAt(0))
      if (s === null || s === undefined) return undefined // null = undefined
      return s === '1' // 1 = true, 0 = false
    }
  }

  return next()
}

export class MysqlDB implements CommonDB {
  constructor(cfg: MysqlDBCfg = {}) {
    this.cfg = {
      typeCast,
      // charset: 'utf8mb4', // for emoji support
      // host: 'localhost',
      // user: MYSQL_USER,
      // password: MYSQL_PW,
      // database: MYSQL_DB,
      ...cfg,
    }
  }

  cfg!: MysqlDBCfg

  async ping(): Promise<void> {
    const con = await this.getConnection()
    con.release()
  }

  async resetCache(table?: string): Promise<void> {}

  @memo()
  pool(): Pool {
    const pool = mysql.createPool(this.cfg)
    const { host, database } = this.cfg
    log(`connected to ${white(host + '/' + database)}`)

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
    return records.map(r => _mapKeys(r, (_v, k) => mapNameFromMySQL(k)) as any)
  }

  // QUERY
  async runQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opt: MysqlDBOptions = {},
  ): Promise<RunQueryResult<DBM>> {
    const sql = dbQueryToSQLSelect(q)
    const records = await this.runSQL<DBM[]>({ sql })
    return {
      records: records.map(
        r => _mapKeys(filterUndefinedValues(r, true), (_v, k) => mapNameFromMySQL(k)) as any,
      ),
    }
  }

  async runSQL<RESULT>(q: QueryOptions): Promise<RESULT> {
    if (this.cfg.logSQL) log(q.sql, q.values)

    return new Promise(async (resolve, reject) => {
      this.pool().query(q, (err, res) => {
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
    _dbms: DBM[],
    opt?: MysqlDBSaveOptions,
  ): Promise<void> {
    if (!_dbms.length) return

    // Stringify object values
    const dbms = _dbms.map(dbm =>
      _mapValues(dbm, v => {
        return v && typeof v === 'object' && !Buffer.isBuffer(v) ? JSON.stringify(v) : v
      }),
    )

    // inserts are split into multiple sentenses to respect the max_packet_size (1Mb usually)
    const sqls = insertSQL(table, dbms)

    for await (const sql of sqls) {
      await this.runSQL({ sql })
    }
  }

  // DELETE
  /**
   * Limitation: always returns [], regardless of which rows are actually deleted
   */
  async deleteByIds(table: string, ids: string[], opt?: MysqlDBOptions): Promise<number> {
    if (!ids.length) return 0
    const sql = dbQueryToSQLDelete(new DBQuery(table).filterEq('id', ids))
    const res = await this.runSQL<any>({ sql })
    return res.affectedRows
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const sql = dbQueryToSQLDelete(q)
    const res = await this.runSQL<any>({ sql })
    return res.affectedRows
  }

  /**
   * Use with caution!
   */
  async dropTable(table: string): Promise<void> {
    await this.runSQL({ sql: `DROP TABLE IF EXISTS ${table}` })
  }

  /**
   * dropIfExists=true needed as a safety check
   */
  async createTable(schema: CommonSchema, opt: CommonDBCreateOptions = {}): Promise<void> {
    if (opt.dropIfExists) await this.dropTable(schema.table)

    const sql = commonSchemaToMySQLDDL(schema)
    await this.runSQL({ sql })
  }

  async getTables(): Promise<string[]> {
    return (await this.runSQL<object[]>({ sql: `show tables` }))
      .map(r => Object.values(r)[0])
      .filter(Boolean)
  }

  async getTableSchema<DBM extends SavedDBEntity>(table: string): Promise<CommonSchema<DBM>> {
    const statsArray = await this.runSQL<MySQLTableStats[]>({
      sql: `describe ${mysql.escapeId(table)}`,
    })

    return {
      table,
      fields: statsArray.map(stats => mysqlTableStatsToCommonSchemaField(stats)),
    }
  }
}
