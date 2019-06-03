import {
  BaseDBEntity,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBQuery,
} from '@naturalcycles/db-lib'
import { logMethod, memo } from '@naturalcycles/js-lib'
import { Pool, PoolConfig, PoolConnection } from 'mysql'
import * as mysql from 'mysql'
import { Observable, Subject } from 'rxjs'
import { Readable, Transform } from 'stream'
import { promisify } from 'util'
import { dbQueryToSQLDelete, dbQueryToSQLSelect } from './query.util'
import { streamToObservable } from './stream.util'

export interface MysqlDBOptions extends CommonDBOptions {}
export interface MysqlDBSaveOptions extends CommonDBSaveOptions {}

export interface MysqlDBCfg extends PoolConfig {}

export class MysqlDB implements CommonDB {
  constructor (private cfg: MysqlDBCfg) {}

  init (): void {
    this.pool()
  }

  @memo()
  @logMethod({ logResult: false })
  pool (): Pool {
    return mysql.createPool(this.cfg)
  }

  async close (): Promise<void> {
    const pool = this.pool()
    await promisify(pool.end.bind(pool))
  }

  async getConnection (): Promise<PoolConnection> {
    const pool = this.pool()
    return promisify(pool.getConnection.bind(pool))()
  }

  // GET
  async getByIds<DBM = any> (table: string, ids: string[], opts?: MysqlDBOptions): Promise<DBM[]> {
    const q = new DBQuery<DBM>(table).filterEq('id', ids)
    return this.runQuery(q, opts)
  }

  // QUERY
  async runQuery<DBM = any> (q: DBQuery<DBM>, opts?: MysqlDBOptions): Promise<DBM[]> {
    const sql = dbQueryToSQLSelect(q)
    return this.runSQL<DBM>(sql)
  }

  async runSQL<DBM = any> (sql: string, opts?: MysqlDBOptions): Promise<DBM[]> {
    return new Promise(async (resolve, reject) => {
      const con = await this.getConnection()
      con.query(sql, (err, res) => {
        con.release()
        if (err) return reject(err)
        resolve(res)
      })
    })
  }

  async runQueryCount<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number> {
    const [row] = await this.runQuery<{ _count: number }>(q.select(['count(*) as _count']))
    return row._count
  }

  streamQuery<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    const subj = new Subject<DBM>()

    const sql = dbQueryToSQLSelect(q)
    this.streamSQL(sql)
      .then(stream => {
        // pipe stream into previously created Subject
        streamToObservable<DBM>(stream).subscribe(subj)
      })
      .catch(err => {
        subj.error(err)
      })

    return subj
  }

  private async streamSQL (sql: string): Promise<Readable> {
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
  async saveBatch<DBM extends BaseDBEntity = any> (
    table: string,
    dbms: DBM[],
    opts?: MysqlDBSaveOptions,
  ): Promise<DBM[]> {
    // todo
    return undefined as any
  }

  // DELETE
  async deleteBy (
    table: string,
    by: string,
    value: any,
    limit = 0,
    opts?: MysqlDBOptions,
  ): Promise<string[]> {
    const sql = dbQueryToSQLDelete(new DBQuery(table).filterEq(by, value).limit(limit))
    await this.runSQL(sql)
    return []
  }

  /**
   * Limitation: always returns [], regardless of which rows are actually deleted
   */
  async deleteByIds (table: string, ids: string[], opts?: MysqlDBOptions): Promise<string[]> {
    const sql = dbQueryToSQLDelete(new DBQuery(table).filterEq('id', ids))
    await this.runSQL(sql)
    return []
    // todo: affectedRows
  }
}
