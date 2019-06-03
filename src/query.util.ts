import { DBQuery } from '@naturalcycles/db-lib'
import * as mysql from 'mysql'

export function dbQueryToSQLSelect (q: DBQuery): string {
  const tokens = selectTokens(q)

  // filters
  tokens.push(...whereTokens(q))

  // order
  tokens.push(...orderTokens(q))

  // limit
  tokens.push(...limitTokens(q))

  return tokens.join(' ')
}

export function dbQueryToSQLDelete (q: DBQuery): string {
  const tokens = [`DELETE FROM`, mysql.escapeId(q.table)]

  // filters
  tokens.push(...whereTokens(q))

  // limit
  tokens.push(...limitTokens(q))

  return tokens.join(' ')
}

function selectTokens (q: DBQuery): string[] {
  const fields = (q._selectedFieldNames || []).length ? (q._selectedFieldNames as string[]) : ['*']

  return [`SELECT`, fields.join(', '), `FROM`, mysql.escapeId(q.table)]
}

function limitTokens (q: DBQuery): string[] {
  if (!q._limitValue) return []
  return [`LIMIT`, String(q._limitValue)]
}

function orderTokens (q: DBQuery): string[] {
  if (!q._orders.length) return []
  return [
    `ORDER BY`,
    q._orders.map(o => `\`${o.name}\` ${o.descending ? 'DESC' : 'ASC'}`).join(', '),
  ]
}

function whereTokens (q: DBQuery): string[] {
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
