package sync

import "testing"

func TestGetTables(t *testing.T) {
	cfg := &SyncConfig{
		STable: "trade",
		Src: DataConfig{
			URI: "root:123456@tcp(127.0.0.1:6030)/",
			DB:  "hft",
		},
		Dst: DataConfig{
			URI: "root:123456@tcp(127.0.0.1:6030)/",
			DB:  "hft",
		},
	}
	s, err := NewTaosSync(cfg)
	if err != nil {
		t.Fatal(err.Error())
	}

	err = s.refreshSTableCols()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(s.cols)
	t.Log(s.insertSql)

	tbls, err := s.getTables("trade")
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(tbls)
}
