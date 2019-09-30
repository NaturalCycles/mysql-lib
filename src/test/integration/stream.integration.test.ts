import { requireEnvKeys, streamToObservable } from '@naturalcycles/nodejs-lib'
import { mergeMap } from 'rxjs/operators'
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
  logSQL: true,
  debugConnections: true,
  // debug: true,
  // multipleStatements: true,
})

afterAll(async () => {
  await db.close()
})

jest.setTimeout(10000000)

const crazyQuery = `select * from
(select adddate('1970-01-01', t5.i*100000 + t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) selected_date from
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t5) v;`

test('test1', async () => {
  // const con0 = await db.getConnection()
  // console.log(`con0 ${con0.threadId}`)
  //
  // const con1 = await db.getConnection()
  // console.log(`con1 ${con1.threadId}`)
  //
  // const con2 = await db.getConnection()
  // console.log(`con2 ${con2.threadId}`)
  const con0 = undefined
  const con1 = undefined
  const con2 = undefined

  let count = 0

  // todo: try w/o observable
  const r0Promise = streamToObservable(await db.streamSQL(crazyQuery, { con: con0 }))
    .pipe(
      mergeMap(async res => {
        count++
        if (count % 100000 === 0) {
          console.log({ count })
        }
      }),
    )
    .toPromise()
  // const r0Promise = Promise.resolve()

  console.log('!!!!!')

  const con3 = await db.getConnection()
  // const con3 = await db.createSingleConnection()
  console.log(`con3 ${con3.threadId}`)

  const r1Promise = db.runSQL(`select sleep(3)`, { con: con1 })

  const r2Promise = db.runSQL(`select sleep(3)`, { con: con2 })
  // const con3 = await db.getConnection()
  // db.pool().getConnection()

  console.log('r1 and r2 started')

  const [r1, r2] = await Promise.all([r1Promise, r2Promise])
  console.log({ r1, r2 })
  const [r0] = await Promise.all([r0Promise])
  console.log({ r0 })
  // const r = await streamToObservable(await db.streamSQL(`select sleep(10)`))
  //   .pipe(mergeMap(async res => {
  //
  //   }))
  //   .toPromise()
  // // const r = await db.runSQL(`select sleep(10)`)
  // console.log(r)
})
