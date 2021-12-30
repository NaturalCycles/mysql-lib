import { Transform } from 'stream'
import { CommonDBCreateOptions, CommonKeyValueDB, KeyValueDBTuple } from '@naturalcycles/db-lib'
import { pMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { QueryOptions } from 'mysql'
import { MysqlDB, MysqlDBCfg } from './mysql.db'

interface KeyValueObject {
  id: string
  v: Buffer
}

export class MySQLKeyValueDB implements CommonKeyValueDB {
  constructor(public cfg: MysqlDBCfg) {
    this.db = new MysqlDB(this.cfg)
  }

  db: MysqlDB

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

  async getByIds(table: string, ids: string[]): Promise<KeyValueDBTuple[]> {
    if (!ids.length) return []

    const sql = `SELECT id,v FROM ${table} where id in (${ids.map(id => `"${id}"`).join(',')})`

    const rows = await this.db.runSQL<KeyValueObject[]>({ sql })

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

  async saveBatch(table: string, entries: KeyValueDBTuple[]): Promise<void> {
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

    return this.db
      .pool()
      .query(sql)
      .stream()
      .pipe(
        new Transform({
          objectMode: true,
          transform(row, _, cb) {
            cb(null, row.id)
          },
        }),
      )
  }

  streamValues(table: string, limit?: number): ReadableTyped<Buffer> {
    let sql = `SELECT v FROM ${table}`
    if (limit) sql += ` LIMIT ${limit}`
    if (this.cfg.logSQL) this.db.cfg.logger.log(`stream: ${sql}`)

    return this.db
      .pool()
      .query(sql)
      .stream()
      .pipe(
        new Transform({
          objectMode: true,
          transform(row, _, cb) {
            cb(null, row.v)
          },
        }),
      )
  }

  streamEntries(table: string, limit?: number): ReadableTyped<KeyValueDBTuple> {
    let sql = `SELECT id,v FROM ${table}`
    if (limit) sql += ` LIMIT ${limit}`
    if (this.cfg.logSQL) this.db.cfg.logger.log(`stream: ${sql}`)

    return this.db
      .pool()
      .query(sql)
      .stream()
      .pipe(
        new Transform({
          objectMode: true,
          transform(row, _, cb) {
            cb(null, [row.id, row.v])
          },
        }),
      )
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
}
