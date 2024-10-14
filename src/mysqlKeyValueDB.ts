import {
  CommonDBCreateOptions,
  CommonKeyValueDB,
  commonKeyValueDBFullSupport,
} from '@naturalcycles/db-lib'
import { IncrementTuple } from '@naturalcycles/db-lib/dist/kv/commonKeyValueDB'
import { AppError, KeyValueTuple, ObjectWithId, pMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { QueryOptions } from 'mysql'
import { MysqlDB, MysqlDBCfg } from './mysql.db'

interface KeyValueObject<V> {
  id: string
  v: V
}

export class MySQLKeyValueDB implements CommonKeyValueDB {
  constructor(public cfg: MysqlDBCfg) {
    this.db = new MysqlDB(this.cfg)
  }

  db: MysqlDB

  support = {
    ...commonKeyValueDBFullSupport,
    increment: false,
  }

  async ping(): Promise<void> {
    await this.db.ping()
  }

  async close(): Promise<void> {
    await this.db.close()
  }

  async createTable(table: string, opt: CommonDBCreateOptions = {}): Promise<void> {
    if (opt.dropIfExists) await this.dropTable(table)

    // On blob sizes: https://tableplus.com/blog/2019/10/tinyblob-blob-mediumblob-longblob.html
    // LONGBLOB supports up to 4gb
    // MEDIUMBLOB up to 16Mb
    const sql = `create table ${table} (id VARCHAR(64) PRIMARY KEY, v LONGBLOB NOT NULL)`
    this.db.cfg.logger.log(sql)
    await this.db.runSQL({ sql })
  }

  async getByIds<V>(table: string, ids: string[]): Promise<KeyValueTuple<string, V>[]> {
    if (!ids.length) return []

    const sql = `SELECT id,v FROM ${table} where id in (${ids.map(id => `"${id}"`).join(',')})`

    const rows = await this.db.runSQL<KeyValueObject<V>[]>({ sql })

    return rows.map(({ id, v }) => [id, v])
  }

  /**
   * Use with caution!
   */
  async dropTable(table: string): Promise<void> {
    await this.db.runSQL({ sql: `DROP TABLE IF EXISTS ${table}` })
  }

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    const sql = `DELETE FROM ${table} WHERE id in (${ids.map(id => `"${id}"`).join(',')})`
    if (this.cfg.logSQL) this.db.cfg.logger.log(sql)
    await this.db.runSQL({ sql })
  }

  async saveBatch<V>(table: string, entries: KeyValueTuple<string, V>[]): Promise<void> {
    const statements: QueryOptions[] = entries.map(([id, buf]) => {
      return {
        sql: `INSERT INTO ${table} (id, v) VALUES (?, ?)`,
        values: [id, buf],
      }
    })

    await pMap(statements, async statement => {
      if (this.cfg.debug) this.db.cfg.logger.log(statement.sql)
      await this.db.runSQL(statement)
    })
  }

  streamIds(table: string, limit?: number): ReadableTyped<string> {
    let sql = `SELECT id FROM ${table}`
    if (limit) sql += ` LIMIT ${limit}`
    if (this.cfg.logSQL) this.db.cfg.logger.log(`stream: ${sql}`)

    return (this.db.pool().query(sql).stream() as ReadableTyped<ObjectWithId>).map(row => row.id)
  }

  streamValues<V>(table: string, limit?: number): ReadableTyped<V> {
    let sql = `SELECT v FROM ${table}`
    if (limit) sql += ` LIMIT ${limit}`
    if (this.cfg.logSQL) this.db.cfg.logger.log(`stream: ${sql}`)

    return (this.db.pool().query(sql).stream() as ReadableTyped<{ v: V }>).map(row => row.v)
  }

  streamEntries<V>(table: string, limit?: number): ReadableTyped<KeyValueTuple<string, V>> {
    let sql = `SELECT id,v FROM ${table}`
    if (limit) sql += ` LIMIT ${limit}`
    if (this.cfg.logSQL) this.db.cfg.logger.log(`stream: ${sql}`)

    return (this.db.pool().query(sql).stream() as ReadableTyped<KeyValueObject<V>>).map(row => [
      row.id,
      row.v,
    ])
  }

  async beginTransaction(): Promise<void> {
    await this.db.runSQL({ sql: `BEGIN TRANSACTION` })
  }

  async endTransaction(): Promise<void> {
    await this.db.runSQL({ sql: `END TRANSACTION` })
  }

  async count(table: string): Promise<number> {
    const sql = `SELECT count(*) as cnt FROM ${table}`
    if (this.cfg.logSQL) this.db.cfg.logger.log(sql)

    const rows = await this.db.runSQL<{ cnt: number }[]>({ sql })
    return rows[0]!.cnt
  }

  async increment(_table: string, _id: string, _by?: number): Promise<number> {
    throw new AppError('MySQLKeyValueDB.increment() is not implemented')
  }

  async incrementBatch(_table: string, _entries: IncrementTuple[]): Promise<IncrementTuple[]> {
    throw new AppError('MySQLKeyValueDB.incrementBatch() is not implemented')
  }
}
