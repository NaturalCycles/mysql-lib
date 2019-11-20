import {
  createTestItemsDBM,
  DATA_TYPE,
  DBQuery,
  getTestItemSchema,
  runCommonDaoTest,
  runCommonDBTest,
  TEST_TABLE,
} from '@naturalcycles/db-lib'
import { requireEnvKeys, unzipToString, zipString } from '@naturalcycles/nodejs-lib'
import { MysqlDB } from '../../mysql.db'

require('dotenv').config()

const { MYSQL_HOST, MYSQL_USER, MYSQL_PW, MYSQL_DB } = requireEnvKeys(
  'MYSQL_HOST',
  'MYSQL_USER',
  'MYSQL_PW',
  'MYSQL_DB',
)

const db = new MysqlDB({
  // connectionLimit: 40,
  // queueLimit: 1,
  host: MYSQL_HOST,
  user: MYSQL_USER,
  password: MYSQL_PW,
  database: MYSQL_DB,
  charset: 'utf8mb4',
  logSQL: true,
  // debugConnections: true,
  // debug: true,
  // multipleStatements: true,
})

beforeAll(async () => {
  await db.createTable(getTestItemSchema(), { dropIfExists: true })
})

afterAll(async () => {
  await db.close()
})

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))

test('getTableSchema', async () => {
  // console.log(await db.getTables())
  const schema = await db.getTableSchema(TEST_TABLE)
  console.log(schema)
  expect(getTestItemSchema()).toMatchObject(schema)
})

test('emojis', async () => {
  const items = createTestItemsDBM(5).map(r => ({ ...r, k1: `😣` }))
  // should not throw
  await db.saveBatch(TEST_TABLE, items)
})

test('fieldName with dot', async () => {
  const fieldName = 'field.with.dot'
  const table = TEST_TABLE + '2'

  const items = createTestItemsDBM(5).map(r => ({ ...r, [fieldName]: 'vv' }))
  const schema = getTestItemSchema()
  schema.fields.push({
    name: fieldName,
    type: DATA_TYPE.STRING,
  })
  schema.table = table

  await db.createTable(schema, { dropIfExists: true })
  await db.saveBatch(table, items)
  const { records } = await db.runQuery(new DBQuery(table))
  // console.log(items2)
  expect(records).toEqual(items)
})

test('buffer', async () => {
  const table = TEST_TABLE + '2'

  const extra = await zipString('hello buffer')

  const items = createTestItemsDBM(5).map(r => ({ ...r, extra }))

  const schema = getTestItemSchema()
  schema.fields.push({
    name: 'extra',
    type: DATA_TYPE.BINARY,
  })
  schema.table = table

  await db.createTable(schema, { dropIfExists: true })
  await db.saveBatch(table, items)
  const { records } = await db.runQuery(new DBQuery(table))
  // console.log(items2)
  console.log(await unzipToString(records[0].extra))
  expect(records).toEqual(items)
})

test('stringify objects', async () => {
  const [item] = createTestItemsDBM(1)
  item.k1 = { some: 'obj', c: 'd', e: 5 } as any

  await db.createTable(getTestItemSchema(), { dropIfExists: true })
  await db.saveBatch(TEST_TABLE, [item])
  const { records } = await db.runQuery(new DBQuery(TEST_TABLE))
  // console.log(records)
  expect(records).toEqual([
    {
      ...item,
      k1: JSON.stringify(item.k1),
    },
  ])
})

test('boolean undefined', async () => {
  const accs = await db.getByIds<any>('Account', [
    '005734f1979f4ca890662c0b9f296dc2',
    '0000a67c9c034e379a938f915766540b',
    '0002ea1a93e84f12bbc12a306f452427',
  ])
  // console.log(accs.map(acc => acc.appConsent))
  console.log(
    accs.map(acc => ({
      id: acc.id,
      appConsent: acc.appConsent,
    })),
  )
  // expect undefined, true, false
})
