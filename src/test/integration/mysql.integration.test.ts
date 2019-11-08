import {
  createTestItemsDBM,
  getTestItemSchema,
  runCommonDaoTest,
  runCommonDBTest,
  TEST_TABLE,
} from '@naturalcycles/db-lib'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
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
  // logSQL: true,
  // debugConnections: true,
  // debug: true,
  // multipleStatements: true,
})

beforeAll(async () => {
  await db.createTable(getTestItemSchema(), true)
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
