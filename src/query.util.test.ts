import { DBQuery } from '@naturalcycles/db-lib'
import { dbQueryToSQLDelete, dbQueryToSQLSelect } from './query.util'

test('dbQueryToSQLSelect', () => {
  let sql = dbQueryToSQLSelect(new DBQuery('TBL1'))
  expect(sql).toMatchSnapshot()

  sql = dbQueryToSQLSelect(
    new DBQuery('TBL1')
      .filterEq('a', 'b')
      .filter('c', '>', '2019')
      .order('aaa')
      .order('bbb', true)
      .limit(15),
  )
  // console.log(sql)
  expect(sql).toMatchSnapshot()

  sql = dbQueryToSQLSelect(new DBQuery('TBL1').filter('num', '>', 15))
  // console.log(sql)
  expect(sql).toMatchSnapshot()

  // NULL cases
  sql = dbQueryToSQLSelect(
    new DBQuery('TBL1')
      .filter('a', '=', undefined)
      .filter('a2', '=', null)
      .filter('a3', '>', null),
  )
  // console.log(sql)
  expect(sql).toMatchSnapshot()

  // ARRAY CASES
  sql = dbQueryToSQLSelect(new DBQuery('TBL1').filter('a', '=', ['a1', 'a2', 'a3']))
  // console.log(sql)
  expect(sql).toMatchSnapshot()
})

test('dbQueryToSQLDelete', () => {
  let sql = dbQueryToSQLDelete(new DBQuery('TBL1'))
  expect(sql).toMatchSnapshot()

  sql = dbQueryToSQLDelete(new DBQuery('TBL1').filter('a', '>', null))
  expect(sql).toMatchSnapshot()
})
