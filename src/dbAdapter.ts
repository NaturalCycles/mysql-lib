import { MysqlDB } from './mysql.db'

export function getDBAdapter(cfgStr: string = '{}'): MysqlDB {
  const cfg = JSON.parse(cfgStr)
  return new MysqlDB(cfg)
}
