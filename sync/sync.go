package sync

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

type DataConfig struct {
	URI string
	DB  string
}

type SyncConfig struct {
	Src     DataConfig
	Dst     DataConfig
	STables []string
}

type TableInfo struct {
	TableName  string `db:"table_name"`
	STableName string `db:"stable_name"`
	DBName     string `db:"db_name"`
	Create     string `db:"-"`
}

type ColInfo struct {
	Field  string
	Type   string
	Length int
	Note   string
}

type sTableInfo struct {
	cols   []ColInfo
	insert string
	create string
}

type TaosSync struct {
	cfg     *SyncConfig
	src     *sqlx.DB
	dst     *sqlx.DB
	sTables map[string]*sTableInfo
}

func NewTaosSync(cfg *SyncConfig) (s *TaosSync, err error) {
	s = new(TaosSync)
	s.cfg = cfg
	s.src, err = sqlx.Connect("taosSql", s.cfg.Src.URI)
	if err != nil {
		return
	}
	s.dst, err = sqlx.Connect("taosSql", s.cfg.Dst.URI)
	if err != nil {
		return
	}
	return
}

func (s *TaosSync) Sync(start, end time.Time) (err error) {
	err = s.refreshSTableCols()
	if err != nil {
		err = fmt.Errorf("Sync refreshSTableCols failed: %w", err)
		return
	}
	tbls, err := s.getAllTables()
	if err != nil {
		err = fmt.Errorf("Sync getTables failed: %w", err)
		return
	}
	err = s.prepareTables(tbls)
	if err != nil {
		err = fmt.Errorf("Sync prepareTables failed: %w", err)
		return
	}
	for _, v := range tbls {
		for i := 0; i != 3; i++ {
			err = s.syncOneTable(v, start, end)
			if err != nil {
				err = fmt.Errorf("Sync table %s failed: %w", v.TableName, err)
				logrus.Errorf("Sync table %s failed: %w, %d times", v.TableName, err, i)
				continue
			} else {
				break
			}
		}
	}
	return
}

func (s *TaosSync) getAllTables() (tables map[string]TableInfo, err error) {
	tables = make(map[string]TableInfo)
	var temp map[string]TableInfo
	for _, v := range s.cfg.STables {
		temp, err = s.getTables(v)
		if err != nil {
			return
		}
		for k, t := range temp {
			tables[k] = t
		}
	}
	return
}

func (s *TaosSync) getTables(stable string) (tables map[string]TableInfo, err error) {
	rows, err := s.src.Queryx("select table_name,db_name,stable_name from INFORMATION_SCHEMA.INS_TABLES where db_name='?' and stable_name='?'", s.cfg.Src.DB, stable)
	if err != nil {
		err = fmt.Errorf("getTables failed:%w", err)
		return
	}
	tables = make(map[string]TableInfo)
	var tbl TableInfo
	var sql string
	var tbls []TableInfo
	for rows.Next() {
		err = rows.StructScan(&tbl)
		if err != nil {
			err = fmt.Errorf("getTables StructScan failed:%w", err)
			break
		}
		tbls = append(tbls, tbl)
	}
	rows.Close()
	if err != nil {
		return
	}
	for _, v := range tbls {
		info := make(map[string]interface{})
		row := s.src.QueryRowx("SHOW CREATE TABLE ?.?", v.DBName, v.TableName)
		err = row.MapScan(info)
		if err != nil {
			return
		}
		// fmt.Println("table info:", info)
		sql = info["Create Table"].(string)
		v.Create = strings.Replace(sql, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
		tables[v.TableName] = v
	}
	rows.Close()

	return
}

func (s *TaosSync) prepareTables(tables map[string]TableInfo) (err error) {
	s.dst.Exec("use ?", s.cfg.Dst.DB)
	for _, v := range s.sTables {
		_, err = s.dst.Exec(v.create)
		if err != nil {
			return
		}
	}
	for _, v := range tables {
		_, err = s.dst.Exec(v.Create)
		if err != nil {
			return
		}
	}
	return
}

func (s *TaosSync) syncOneTable(tbl TableInfo, start, end time.Time) (err error) {
	s.src.Exec("use ?", s.cfg.Src.DB)
	s.dst.Exec("use ?", s.cfg.Dst.DB)
	logrus.Infof("sync %s, %s - %s", tbl.TableName, start.Format(time.RFC3339), end.Format(time.RFC3339))

	temp := make(map[string]interface{})
	err = s.src.QueryRowx("select count(*) as total from ? where ts >= '?' and ts<='?'", tbl.TableName, start.Format(time.RFC3339), end.Format(time.RFC3339)).MapScan(temp)
	if err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			logrus.Infof("skip no datas: %s, %s - %s", tbl.TableName, start.Format(time.RFC3339), end.Format(time.RFC3339))
			err = nil
			return
		}
		err = fmt.Errorf("count table failed: %w", err)
		return
	}
	nSrcTotal := temp["total"].(int64)

	rows, err := s.src.Queryx("select * from ? where ts >= '?' and ts<='?'", tbl.TableName, start.Format(time.RFC3339), end.Format(time.RFC3339))
	if err != nil {
		return
	}
	defer rows.Close()
	cache := make([]map[string]interface{}, 100)
	i := 0
	var nTotalCount int64
	for rows.Next() {
		nTotalCount++
		data := make(map[string]interface{})
		err = rows.MapScan(data)
		if err != nil {
			return
		}
		cache[i] = data
		i++
		if i >= 100 {
			err = s.writeOnce(tbl, cache)
			if err != nil {
				return
			}
			i = 0
		}

	}
	if i > 0 {
		err = s.writeOnce(tbl, cache[0:i])
		if err != nil {
			return
		}
	}
	if nTotalCount != nSrcTotal {
		logrus.Errorf("src has %d, but only sync %d", nSrcTotal, nTotalCount)
		err = fmt.Errorf("count not match")
		return
	}
	logrus.Infof("%s success sync total %d", tbl.TableName, nTotalCount)
	return
}

func (s *TaosSync) writeOnce(tbl TableInfo, datas []map[string]interface{}) (err error) {
	info, ok := s.sTables[tbl.STableName]
	if !ok {
		err = fmt.Errorf("writeOnce no such stable info:%s", tbl.STableName)
		return
	}
	sql := strings.Replace(info.insert, "?", tbl.TableName, 1)
	_, err = s.dst.NamedExec(sql, datas)
	return
}

func (s *TaosSync) refreshSTableCols() (err error) {
	sTblCols := make(map[string]*sTableInfo)
	var sql string
	for _, st := range s.cfg.STables {
		info := make(map[string]interface{})
		row := s.src.QueryRowx("SHOW CREATE STABLE ?.?", s.cfg.Src.DB, st)
		err = row.MapScan(info)
		if err != nil {
			return
		}
		sql = info["Create Table"].(string)
		sql = strings.Replace(sql, "CREATE STABLE", "CREATE STABLE IF NOT EXISTS", 1)
		var cols []ColInfo
		err = s.src.Select(&cols, "DESCRIBE ?.?", s.cfg.Src.DB, st)
		if err != nil {
			return
		}
		sTblCols[st] = &sTableInfo{cols: cols, create: sql}
		var colSql, valueSql string
		for _, v := range cols {
			if v.Note == "TAG" {
				continue
			}
			if v.Type == "VARCHAR" {
				valueSql += fmt.Sprintf("':%s',", v.Field)
			} else {
				valueSql += fmt.Sprintf(":%s,", v.Field)
			}
			colSql += v.Field + ","

		}
		sTblCols[st].insert = fmt.Sprintf("insert into ? (%s) VALUES(%s)", strings.TrimRight(colSql, ","), strings.TrimRight(valueSql, ","))
		logrus.Infof("%s insert sql: %s", st, sTblCols[st].insert)
	}
	s.sTables = sTblCols
	return
}
