import { runCommonDaoTest, runCommonDBTest } from '@naturalcycles/db-lib'
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
  connectionLimit: 40,
  host: MYSQL_HOST,
  user: MYSQL_USER,
  password: MYSQL_PW,
  database: MYSQL_DB,
  logSQL: true,
  // debug: true,
  // multipleStatements: true,
})

beforeAll(async () => {
  await db.runSQL(`DROP TABLE IF EXISTS test_table`)

  await db.runSQL(`
CREATE TABLE test_table (
  id varchar(255) NOT NULL,
  created int(11) DEFAULT NULL,
  updated int(11) DEFAULT NULL,
  _ver int(11) DEFAULT NULL,
  k1 varchar(255) DEFAULT NULL,
  k2 varchar(255) DEFAULT NULL,
  k3 int(11) DEFAULT NULL,
  even boolean default null,
  PRIMARY KEY (id)
) ENGINE=InnoDB`)
})

afterAll(async () => {
  await db.close()
})

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))
