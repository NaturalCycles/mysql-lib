import { CommonSchema, CommonSchemaField, DATA_TYPE } from '@naturalcycles/db-lib'
import { filterFalsyValues } from '@naturalcycles/js-lib'
import * as mysql from 'mysql'

export interface MySQLTableStats {
  Field: string // created
  Type: string // int(11)
  Null: string // 'YES'
}

export interface MySQLSchemaOptions {
  /**
   * @default 'InnoDB'
   */
  engine?: string
}

/**
 * It currently skips nullability and declares everything as "DEFAULT NULL".
 */
export function commonSchemaToMySQLDDL(schema: CommonSchema, opt: MySQLSchemaOptions = {}): string {
  const { engine = 'InnoDB' } = opt

  const { table, fields } = schema

  const lines: string[] = [`CREATE TABLE ${mysql.escapeId(table)} (`]

  const innerLines = fields.map(f => commonSchemaFieldToDDL(f))
  innerLines.push(`PRIMARY KEY (id)`)

  lines.push(innerLines.join(',\n'))
  lines.push(`) ENGINE=${engine}`)

  return lines.join('\n')
}

const typeToMySQLType: Record<DATA_TYPE, string> = {
  [DATA_TYPE.STRING]: `LONGTEXT`,
  [DATA_TYPE.LOCAL_DATE]: `VARCHAR(255)`,
  [DATA_TYPE.INT]: `INT(11)`,
  [DATA_TYPE.TIMESTAMP]: `INT(11)`,
  [DATA_TYPE.FLOAT]: `FLOAT(11)`,
  [DATA_TYPE.BOOLEAN]: `TINYINT(1)`,
  [DATA_TYPE.BINARY]: `LONGBLOB`,
  [DATA_TYPE.ARRAY]: `LONGTEXT`, // will be JSON.stringified
  [DATA_TYPE.OBJECT]: `LONGTEXT`, // will be JSON.stringified
  [DATA_TYPE.NULL]: `VARCHAR(255)`,
  [DATA_TYPE.UNKNOWN]: `LONGTEXT`,
}

function commonSchemaFieldToDDL(f: CommonSchemaField): string {
  if (f.name === 'id') {
    return `id VARCHAR(255) NOT NULL`
  }

  const tokens: string[] = [
    mysql.escapeId(mapNameToMySQL(f.name)),
    typeToMySQLType[f.type] || typeToMySQLType[DATA_TYPE.UNKNOWN],
    `DEFAULT NULL`,
  ]

  return tokens.join(' ')
}

export function mysqlTableStatsToCommonSchemaField(s: MySQLTableStats): CommonSchemaField {
  const name = mapNameFromMySQL(s.Field)
  const notNull = (s.Null || '').toUpperCase() !== 'YES'

  let type: DATA_TYPE = DATA_TYPE.UNKNOWN

  const t = (s.Type || '').toLowerCase()

  if (t) {
    if (t.includes('text') || t.includes('char')) {
      type = DATA_TYPE.STRING
    } else if (t.includes('lob')) {
      type = DATA_TYPE.BINARY
    } else if (t.startsWith('tinyint') || t.includes('(1)')) {
      type = DATA_TYPE.BOOLEAN
    } else if (t.startsWith('int(')) {
      type = DATA_TYPE.INT
    } else if (t.startsWith('float')) {
      type = DATA_TYPE.FLOAT
    }
  }

  return filterFalsyValues({
    name,
    type,
    notNull,
  })
}

/**
 * Because MySQL doesn't support `.` in field names and escapes them as tableName + fieldName.
 * @param name
 */
export function mapNameToMySQL(name: string): string {
  return name.replace(/\./g, '_dot_')
}

export function mapNameFromMySQL(name: string): string {
  return name.replace(/_dot_/g, '.')
}
