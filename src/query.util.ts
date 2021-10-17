import { DBQuery } from '@naturalcycles/db-lib'
import { DBQueryFilterOperator } from '@naturalcycles/db-lib/src/query/dbQuery'
import { _hb } from '@naturalcycles/js-lib'
import { white, yellow } from '@naturalcycles/nodejs-lib/dist/colors'
import { QueryOptions } from 'mysql'
import * as mysql from 'mysql'
import { mapNameToMySQL } from './schema/mysql.schema.util'

const MAX_PACKET_SIZE = 1024 * 1024 // 1Mb
const MAX_ROW_SIZE = 800 * 1024 // 1Mb - margin

export function dbQueryToSQLSelect(q: DBQuery<any>): string {
  const tokens = selectTokens(q)

  // filters
  tokens.push(...whereTokens(q))

  // order
  tokens.push(...orderTokens(q))

  // offset/limit
  tokens.push(...offsetLimitTokens(q))

  return tokens.join(' ')
}

export function dbQueryToSQLDelete(q: DBQuery<any>): string {
  const tokens = [`DELETE FROM`, mysql.escapeId(q.table)]

  // filters
  tokens.push(...whereTokens(q))

  // offset/limit
  tokens.push(...offsetLimitTokens(q))

  return tokens.join(' ')
}

/**
 * Returns array of sql statements to respect the max sql size.
 */
export function insertSQL(
  table: string,
  records: Record<any, any>[],
  verb: 'INSERT' | 'REPLACE' = 'INSERT',
): string[] {
  // INSERT INTO table_name (column1, column2, column3, ...)
  // VALUES (value1, value2, value3, ...);

  // eslint-disable-next-line unicorn/no-array-reduce
  const fieldSet = records.reduce((set: Set<string>, rec) => {
    Object.keys(rec).forEach(field => set.add(field))
    return set
  }, new Set<string>())
  const fields = [...fieldSet]

  const start = [
    verb,
    `INTO`,
    mysql.escapeId(table),
    `(` + [...fields].map(f => mysql.escapeId(mapNameToMySQL(f))).join(',') + `)`,
    `VALUES\n`,
  ].join(' ')

  const valueRows = records.map(rec => {
    return `(` + fields.map(k => mysql.escape(rec[k])).join(',') + `)`
  })

  const full = start + valueRows.join(',\n')

  if (full.length < MAX_PACKET_SIZE) return [full]

  const sqls: string[] = []
  let sql: string | undefined

  valueRows.forEach(vrow => {
    if (!sql) {
      sql = start + vrow
    } else {
      if (sql.length + vrow.length >= MAX_ROW_SIZE) {
        sqls.push(sql)
        sql = start + vrow // reset
      } else {
        sql += ',\n ' + vrow // add
      }
    }
  })

  if (sql) {
    sqls.push(sql) // last one
  }

  console.log(
    `${white(table)} large sql query (${yellow(_hb(full.length))}) was split into ${yellow(
      sqls.length,
    )} chunks`,
  )

  return sqls

  // todo: handle "upsert" later
}

export function insertSQLSingle(table: string, record: Record<any, any>): string {
  // INSERT INTO table_name (column1, column2, column3, ...)
  // VALUES (value1, value2, value3, ...);

  const tokens = [
    `INSERT INTO`,
    mysql.escapeId(table),
    `(` +
      Object.keys(record)
        .map(f => mysql.escapeId(mapNameToMySQL(f)))
        .join(',') +
      `)`,
    `\nVALUES`,
    `(` +
      Object.values(record)
        .map(v => mysql.escapeId(v))
        .join(',') +
      `)`,
  ]

  return tokens.join(' ')
}

export function insertSQLSetSingle(table: string, record: Record<any, any>): QueryOptions {
  return {
    sql: `INSERT INTO ${mysql.escapeId(table)} SET ?`,
    values: [record],
  }
}

export function dbQueryToSQLUpdate(q: DBQuery<any>, record: Record<any, any>): string {
  // var sql = mysql.format('UPDATE posts SET modified = ? WHERE id = ?', [CURRENT_TIMESTAMP, 42]);
  const tokens = [
    `UPDATE`,
    mysql.escapeId(q.table),
    `SET`,
    Object.keys(record)
      .map(f => mysql.escapeId(mapNameToMySQL(f)) + ' = ?')
      .join(', '),
    ...whereTokens(q),
  ]

  return mysql.format(tokens.join(' '), Object.values(record))
}

function selectTokens(q: DBQuery): string[] {
  let fields = ['*']

  if (q._selectedFieldNames) {
    fields = q._selectedFieldNames.length ? (q._selectedFieldNames as string[]) : ['id']
  }

  // We don't do `escapeId` cause it'll ruin e.g SELECT `count *` FROM ...
  return [`SELECT`, fields.map(f => mapNameToMySQL(f)).join(', '), `FROM`, mysql.escapeId(q.table)]
}

function offsetLimitTokens(q: DBQuery<any>): string[] {
  const tokens: string[] = []

  if (q._limitValue) {
    tokens.push(`LIMIT`, String(q._limitValue))

    // In SQL OFFSET is only allowed if LIMIT is set
    if (q._offsetValue) {
      tokens.push(`OFFSET`, String(q._offsetValue))
    }
  }

  return tokens
}

function orderTokens(q: DBQuery): string[] {
  if (!q._orders.length) return []
  return [
    `ORDER BY`,
    q._orders.map(o => `\`${o.name}\` ${o.descending ? 'DESC' : 'ASC'}`).join(', '),
  ]
}

const OP_MAP: Partial<Record<DBQueryFilterOperator, string>> = {
  '==': '=',
}

function whereTokens(q: DBQuery): string[] {
  if (!q._filters.length) return []

  return [
    `WHERE`,
    q._filters
      .map(f => {
        if (f.val === null || f.val === undefined) {
          // special treatment

          return [
            mysql.escapeId(mapNameToMySQL(f.name as string)),
            f.op === '==' ? 'IS NULL' : 'IS NOT NULL',
          ].join(' ')
        }

        if (Array.isArray(f.val)) {
          // special case for arrays
          return `${mysql.escapeId(mapNameToMySQL(f.name as string))} IN (${mysql.escape(f.val)})`
        }

        return [
          mysql.escapeId(mapNameToMySQL(f.name as string)),
          OP_MAP[f.op] || f.op,
          mysql.escape(f.val),
        ].join(' ')
      })
      .join(' AND '),
  ]
}
