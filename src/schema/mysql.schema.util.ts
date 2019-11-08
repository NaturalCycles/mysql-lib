import { CommonSchema, CommonSchemaField, DATA_TYPE } from '@naturalcycles/db-lib'
import * as mysql from 'mysql'

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
  [DATA_TYPE.BINARY]: `BLOB`,
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
    mysql.escapeId(f.name),
    typeToMySQLType[f.type] || typeToMySQLType[DATA_TYPE.UNKNOWN],
    `DEFAULT NULL`,
  ]

  return tokens.join(' ')
}

// CREATE TABLE test_table (
//   id varchar(255) NOT NULL,
//   created int(11) DEFAULT NULL,
//   updated int(11) DEFAULT NULL,
//   _ver int(11) DEFAULT NULL,
//   k1 varchar(255) DEFAULT NULL,
//   k2 varchar(255) DEFAULT NULL,
//   k3 int(11) DEFAULT NULL,
//   even boolean default null,
//   PRIMARY KEY (id)
// ) ENGINE=InnoDB`)
