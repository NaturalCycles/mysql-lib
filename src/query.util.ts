import { DBQuery } from '@naturalcycles/db-lib'
import * as mysql from 'mysql'

export function dbQueryToSQLSelect(q: DBQuery): string {
  const tokens = selectTokens(q)

  // filters
  tokens.push(...whereTokens(q))

  // order
  tokens.push(...orderTokens(q))

  // limit
  tokens.push(...limitTokens(q))

  return tokens.join(' ')
}

export function dbQueryToSQLDelete(q: DBQuery): string {
  const tokens = [`DELETE FROM`, mysql.escapeId(q.table)]

  // filters
  tokens.push(...whereTokens(q))

  // limit
  tokens.push(...limitTokens(q))

  return tokens.join(' ')
}

export function insertSQL(
  table: string,
  records: object[],
  verb: 'INSERT' | 'REPLACE' = 'INSERT',
): string {
  // INSERT INTO table_name (column1, column2, column3, ...)
  // VALUES (value1, value2, value3, ...);

  const tokens = [verb, `INTO`, mysql.escapeId(table)]

  const fieldSet = records.reduce((set: Set<string>, rec) => {
    Object.keys(rec).forEach(field => set.add(field))
    return set
  }, new Set<string>())
  const fields = [...fieldSet]

  tokens.push(`(` + [...fields].map(f => mysql.escapeId(f)).join(',') + `)`, `VALUES\n`)

  const valueRows = records.map(rec => {
    return `(` + fields.map(k => mysql.escape(rec[k])).join(',') + `)`
  })

  tokens.push(valueRows.join(',\n '))

  return tokens.join(' ')
  // todo: handle "upsert" later
}

export function insertSQLSingle(table: string, record: object): string {
  // INSERT INTO table_name (column1, column2, column3, ...)
  // VALUES (value1, value2, value3, ...);

  const tokens = [
    `INSERT INTO`,
    mysql.escapeId(table),
    `(` +
      Object.keys(record)
        .map(f => mysql.escapeId(f))
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

export function dbQueryToSQLUpdate(q: DBQuery, record: object): string {
  // var sql = mysql.format('UPDATE posts SET modified = ? WHERE id = ?', [CURRENT_TIMESTAMP, 42]);
  const tokens = [
    `UPDATE`,
    mysql.escapeId(q.table),
    `SET`,
    Object.keys(record)
      .map(f => mysql.escapeId(f) + ' = ?')
      .join(', '),
    ...whereTokens(q),
  ]

  return mysql.format(tokens.join(' '), Object.values(record))
}

function selectTokens(q: DBQuery): string[] {
  let fields = ['*']

  if (q._selectedFieldNames) {
    fields = q._selectedFieldNames.length ? q._selectedFieldNames : ['id']
  }

  return [`SELECT`, fields.join(', '), `FROM`, mysql.escapeId(q.table)]
}

function limitTokens(q: DBQuery): string[] {
  if (!q._limitValue) return []
  return [`LIMIT`, String(q._limitValue)]
}

function orderTokens(q: DBQuery): string[] {
  if (!q._orders.length) return []
  return [
    `ORDER BY`,
    q._orders.map(o => `\`${o.name}\` ${o.descending ? 'DESC' : 'ASC'}`).join(', '),
  ]
}

function whereTokens(q: DBQuery): string[] {
  if (!q._filters.length) return []

  return [
    `WHERE`,
    q._filters
      .map(f => {
        if (f.val === null || f.val === undefined) {
          // special treatment

          return [mysql.escapeId(f.name), f.op === '=' ? 'IS NULL' : 'IS NOT NULL'].join(' ')
        }

        if (Array.isArray(f.val)) {
          // special case for arrays
          return `${mysql.escapeId(f.name)} IN (${mysql.escape(f.val)})`
        }

        return [mysql.escapeId(f.name), f.op, mysql.escape(f.val)].join(' ')
      })
      .join(' AND '),
  ]
}
