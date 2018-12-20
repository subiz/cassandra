package cassandra

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
)

func (me *Query) Connect(seeds []string, keyspace string) error {
	me.keyspace = keyspace
	me.table = new(sync.Map)
	cluster := gocql.NewCluster(seeds...)
	cluster.Timeout = 10 * time.Second
	cluster.Keyspace = "system_schema"
	var defaultSession *gocql.Session
	var err error
	for {
		if defaultSession, err = cluster.CreateSession(); err == nil {
			break
		}
		fmt.Println("cassandra", err, ". Retring after 5sec...")
		time.Sleep(5 * time.Second)
	}

	fmt.Println("CONNECTED TO ", seeds)

	defer func() {
		defaultSession.Close()
	}()

	if err = me.loadTables(defaultSession, keyspace); err != nil {
		return err
	}

	cluster.Keyspace = keyspace
	me.Session, err = cluster.CreateSession()
	return err
}

type Query struct {
	keyspace string
	Session  *gocql.Session
	table    *sync.Map
}

func (s *Query) extractFields(query string) []string {
	fs := make([]string, 0)
	columns := strings.Split(query, "\n")

	for _, col := range columns {
		colSplit := strings.Split(col, " ")
		f := strings.TrimSpace(colSplit[0])
		if f == "PRIMARY" || f == "" {
			continue
		}
		fs = append(fs, f)
	}
	return fs
}

func (s *Query) CreateTable(table string, query string, option ...string) error {
	var o string
	if len(option) > 0 {
		o = option[0]
	}
	s.table.Store(table, s.extractFields(query))

	return s.Session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)%s`,
		table, query, o)).Exec()
}

func (s *Query) Delete(table string, query interface{}) error {
	qs, qp, err := s.buildQuery(query)
	if err != nil {
		return err
	}
	if qs == "" {
		return nil
	}
	//	qs = " WHERE " + qs
	querystring := fmt.Sprintf("DELETE FROM %s %s", table, qs)
	return s.Session.Query(querystring, qp...).Exec()
}

func CreateKeyspace(seeds []string, keyspace string, repfactor int) error {
	cluster := gocql.NewCluster(seeds...)
	cluster.Timeout = 10 * time.Second
	cluster.Keyspace = "system_schema"
	var defaultSession *gocql.Session
	var err error
	for {
		if defaultSession, err = cluster.CreateSession(); err == nil {
			break
		}
		fmt.Println("cassandra", err, ". Retring after 5sec...")
		time.Sleep(5 * time.Second)
	}
	fmt.Println("CONNECTED TO ", seeds)
	defer defaultSession.Close()

	return defaultSession.Query(fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class': 'SimpleStrategy',
		'replication_factor': %d
	}`, keyspace, repfactor)).Exec()
}

func (s *Query) loadTables(ss *gocql.Session, keyspace string) error {
	iter := ss.Query(`SELECT table_name, column_name FROM columns WHERE `+
		`keyspace_name=?`, keyspace).Iter()
	var tbl, col string
	km := make(map[string][]string)
	for iter.Scan(&tbl, &col) {
		km[tbl] = append(km[tbl], string(col))
	}
	if err := iter.Close(); err != nil {
		return err
	}

	for tbl, cols := range km {
		s.table.Store(tbl, cols)
	}
	return nil
}

func (s *Query) Upsert(table string, p interface{}) error {
	columns := make([]string, 0)
	phs := make([]string, 0) // place holders
	data := make([]interface{}, 0)

	var valueOf reflect.Value
	var typeOf reflect.Type
	if reflect.TypeOf(p).Kind() == reflect.Ptr {
		valueOf, typeOf = reflect.ValueOf(p).Elem(), reflect.TypeOf(p).Elem()
	} else {
		valueOf, typeOf = reflect.ValueOf(p), reflect.TypeOf(p)
	}
	for i := 0; i < valueOf.NumField(); i++ {
		vf := valueOf.Field(i)
		tf := typeOf.Field(i)

		jsonname := strings.Split(tf.Tag.Get("json"), ",")[0]
		if jsonname == "-" {
			continue
		}

		// only consider field which defined in table
		if tbfields, ok := s.table.Load(table); ok {
			if !containString(tbfields.([]string), "\""+jsonname+"\"") && !containString(tbfields.([]string), jsonname) {
				continue
			}
		}

		if isReservedKeyword(jsonname) {
			jsonname = "\"" + jsonname + "\""
		}

		if reflect.DeepEqual(vf.Interface(), reflect.Zero(vf.Type()).Interface()) {
			continue
		}

		columns = append(columns, jsonname)
		phs = append(phs, "?")

		if vf.Type().Kind() == reflect.Slice && vf.Type().Elem().Kind() == reflect.Ptr {
			bs := make([][]byte, 0, vf.Len())
			for i := 0; i < vf.Len(); i++ {
				b, err := proto.Marshal(vf.Index(i).Interface().(proto.Message))
				if err != nil {
					return err
				}
				bs = append(bs, b)
			}
			data = append(data, bs)
		} else if vf.Type().Kind() == reflect.Ptr && vf.Type().Elem().Kind() == reflect.Struct {
			b, err := proto.Marshal(vf.Interface().(proto.Message))
			if err != nil {
				return err
			}
			data = append(data, b)
		} else {
			data = append(data, vf.Interface())
		}
	}

	querystring := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)", table, strings.Join(columns, ","), strings.Join(phs, ","))
	return s.Session.Query(querystring, data...).Exec()
}

var keywords = []string{"ALL", "ALLOW", "ALTER", "AND", "ANY", "APPLY", "AS", "ASC", "ASCII", "AUTHORIZE", "BATCH", "BEGIN", "BIGINT", "BLOB", "BOOLEAN", "BY", "CLUSTERING", "COLUMNFAMILY", "COMPACT", "CONSISTENCY", "COUNT", "COUNTER", "CREATE", "CUSTOM", "DECIMAL", "DELETE", "DESC", "DISTINCT", "DOUBLE", "DROP", "EACH", "EXISTS", "FILTERING", "FLOAT", "FROM", "FROZEN", "FULL", "GRANT", "IF", "IN", "INDEX", "INET", "INFINITY", "INSERT", "INT", "INTO", "KEY", "KEYSPACE", "KEYSPACES", "LEVEL", "LIMIT", "LIST", "LOCAL", "LOCAL", "MAP", "MODIFY", "NAN", "NORECURSIVE", "NOSUPERUSER", "NOT", "OF", "ON", "ONE", "ORDER", "PASSWORD", "PERMISSION", "PERMISSIONS", "PRIMARY", "QUORUM", "RENAME", "REVOKE", "SCHEMA", "SELECT", "SET", "STATIC", "STORAGE", "SUPERUSER", "TABLE", "TEXT", "TIMESTAMP", "TIMEUUID", "THREE", "TO", "TOKEN", "TRUNCATE", "TTL", "TUPLE", "TWO", "UNLOGGED", "UPDATE", "USE", "USER", "USERS", "USING", "UUID", "VALUES", "VARCHAR", "VARINT", "WHERE", "WITH", "WRITETIME", "VIEW"}

func isReservedKeyword(key string) bool {
	return containString(keywords, strings.ToUpper(key))
}

func (s *Query) buildQuery(query interface{}) (string, []interface{}, error) {
	var m map[string]interface{}
	b, err := json.Marshal(query)
	if err != nil {
		return "", nil, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return "", nil, err
	}

	q := make([]string, 0, len(m))       // query
	qp := make([]interface{}, 0, len(m)) // query parameter
	for k, v := range m {
		nk := k
		if isReservedKeyword(k) {
			nk = "\"" + k + "\""
		}
		q = append(q, nk+"=?")
		switch vwt := v.(type) {
		case float64:
			v = int64(vwt)
		}
		qp = append(qp, v)
	}
	qs := strings.Join(q, " AND ")
	if qs == "" {
		return "", nil, nil
	}
	return " WHERE " + qs, qp, nil
}

func (s *Query) Read(table string, p interface{}, query interface{}) error {
	valueOf := reflect.New(reflect.SliceOf(reflect.TypeOf(p))).Elem()
	cols, findicies := s.analysisType(table, valueOf)
	qs, qp, err := s.buildQuery(query)
	querystring := fmt.Sprintf("SELECT %s FROM %s %s LIMIT 1", cols, table, qs)
	err = s.alloc(valueOf, findicies, querystring, qp)
	if err != nil {
		return err
	}

	if valueOf.Len() == 0 {
		return gocql.ErrNotFound
	}

	reflect.ValueOf(p).Elem().Set(valueOf.Index(0).Elem())
	return nil
}

func (s *Query) buildMapQuery(query map[string]interface{}) (string, []interface{}) {
	q := make([]string, 0, len(query))       // query
	qp := make([]interface{}, 0, len(query)) // query parameter
	for k, v := range query {
		q, qp = append(q, k+"?"), append(qp, v)
	}
	qs := strings.Join(q, " AND ")
	if qs == "" {
		return "", nil
	}
	return " WHERE " + qs, qp
}

// p is pointer to array of pointer
func (s *Query) analysisType(table string, p reflect.Value) (cols string, findices []int) {
	columns := make([]string, 0)
	eleTypeOf := p.Type().Elem().Elem()
	validC := make([]int, 0)
	for i := 0; i < eleTypeOf.NumField(); i++ {
		tf := eleTypeOf.Field(i)
		jsonname := strings.Split(tf.Tag.Get("json"), ",")[0]
		if jsonname == "-" {
			continue
		}
		// only consider column which is defined in table
		if tbfields, ok := s.table.Load(table); ok {
			if !containString(tbfields.([]string), "\""+jsonname+"\"") && !containString(tbfields.([]string), jsonname) {
				continue
			}
		}

		if isReservedKeyword(jsonname) {
			jsonname = "\"" + jsonname + "\""
		}

		validC = append(validC, i)
		columns = append(columns, jsonname)
	}

	return strings.Join(columns, ","), validC
}

func (s *Query) List(table string, p interface{}, query map[string]interface{}, limit int) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return errors.New("cassandra reflect error: p must be a pointer to array")
	}
	valueOf := reflect.ValueOf(p).Elem() // a slice

	if limit == 0 {
		limit = 20
	} else if limit < 0 {
		limit = -limit
	}
	if limit > 1000 {
		limit = 1000
	}

	orderby := ""
	if query["order by"] != nil {
		orderby = "ORDER BY " + query["order by"].(string)
		delete(query, "order by")
	}

	cols, findicies := s.analysisType(table, valueOf)
	qs, qp := s.buildMapQuery(query)

	querystring := fmt.Sprintf("SELECT %s FROM %s %s %s LIMIT %v", cols, table, qs, orderby, limit)
	return s.alloc(valueOf, findicies, querystring, qp)
}

func (s *Query) buildBatchQuery(query map[string]interface{}) (string, []interface{}) {
	q := make([]string, 0) // query
	qp := make([]interface{}, 0)
	for k, v := range query {
		if reflect.Slice == reflect.TypeOf(v).Kind() {
			s := reflect.ValueOf(v)
			if s.Len() == 0 {
				continue
			}
			ph := make([]string, 0, s.Len())
			for i := 0; i < s.Len(); i++ {
				ph = append(ph, "?")
				qp = append(qp, s.Index(i).Interface())
			}
			phs := " (" + strings.Join(ph, ",") + ")"
			q = append(q, k+phs)
		} else {
			q = append(q, k+"?")
			qp = append(qp, v)
		}
	}
	qs := strings.Join(q, " AND ")
	if qs == "" {
		return "", nil
	}
	return " WHERE " + qs, qp
}

func (s Query) DropKeyspace() error {
	querystring := "DROP KEYSPACE IF EXISTS " + s.keyspace
	return s.Session.Query(querystring).Exec()
}

func (s Query) DropView(view string) error {
	querystring := "DROP MATERIALIZED VIEW IF EXISTS " + view
	return s.Session.Query(querystring).Exec()
}

func (s Query) DropTable(table string) error {
	querystring := "DROP TABLE IF EXISTS " + table
	return s.Session.Query(querystring).Exec()
}

func (s *Query) alloc(v reflect.Value, findicies []int, querystring string, qp []interface{}) error {
	val := v //reflect.MakeSlice(v.Type(), 0, 1)
	iter := s.Session.Query(querystring, qp...).Iter()
	ps := reflect.MakeSlice(val.Type(), 1, 1)
	t := val.Type().Elem().Elem()
	for {
		pnewele := reflect.New(t)
		data := make([]interface{}, 0, len(findicies))
		sfis := make(map[int]*[]byte)
		sfises := make(map[int]*[][]byte) // slice of struct fields indexs
		for _, i := range findicies {
			ele := buildPlaceHolder(i, pnewele.Elem(), sfis, sfises)
			data = append(data, ele)
		}

		if !iter.Scan(data...) {
			break
		}

		marshalTo(pnewele.Elem(), sfis, sfises)
		ps.Index(0).Set(pnewele)
		val = reflect.AppendSlice(val, ps)
	}
	v.Set(val)
	return iter.Close()
}

func (s *Query) ReadBatch(table string, p interface{}, query map[string]interface{}) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return errors.New("cassandra reflect error: p must be a pointer to array")
	}
	valueOf := reflect.ValueOf(p).Elem() // a slice
	cols, findicies := s.analysisType(table, valueOf)
	qs, qp := s.buildBatchQuery(query)
	querystring := fmt.Sprintf("SELECT %s FROM %s %s", cols, table, qs)
	return s.alloc(valueOf, findicies, querystring, qp)
}

func unmarshalPointerToStruct(b []byte, tf reflect.Type) (reflect.Value, error) {
	pf := reflect.New(tf.Elem())
	err := proto.Unmarshal(b, pf.Interface().(proto.Message))
	if err != nil {
		return reflect.Value{}, err
	}
	return pf, nil
}

func buildPlaceHolder(i int, f reflect.Value, sfis map[int]*[]byte, sfises map[int]*[][]byte) interface{} {
	vf := f.Field(i)
	if vf.Type().Kind() == reflect.Slice && vf.Type().Elem().Kind() == reflect.Ptr {
		var bs [][]byte
		sfises[i] = &bs
		return &bs
	} else if vf.Type().Kind() == reflect.Ptr && vf.Type().Elem().Kind() == reflect.Struct {
		var b []byte
		sfis[i] = &b
		return &b
	} else {
		return vf.Addr().Interface()
	}
}

func marshalTo(val reflect.Value, sfis map[int]*[]byte, sfises map[int]*[][]byte) error {
	for k, v := range sfis {
		if v == nil {
			continue
		}
		pf, err := unmarshalPointerToStruct(*v, val.Field(k).Type())
		if err != nil {
			return err
		}
		val.Field(k).Set(pf)
	}

	for k, v := range sfises {
		if v == nil {
			continue
		}
		vf := val.Field(k)

		dest := reflect.MakeSlice(vf.Type(), 0, 0)
		ss := reflect.MakeSlice(vf.Type(), 1, 1)
		for _, b := range *v {
			pe := reflect.New(vf.Type().Elem().Elem())
			proto.Unmarshal(b, pe.Interface().(proto.Message))
			ss.Index(0).Set(pe)
			dest = reflect.AppendSlice(dest, ss)
		}
		vf.Set(dest)
	}
	return nil
}

func (s *Query) ListXPar(table string, p interface{}, query map[string]interface{}, parname string, pars []interface{}, limit int) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return errors.New("cassandra reflect error: p must be a pointer to array")
	}
	valueOf := reflect.ValueOf(p).Elem() // a slice

	if limit == 0 {
		limit = 20
	} else if limit < 0 {
		limit = -limit
	}
	if limit > 1000 {
		limit = 1000
	}

	orderby := ""
	if query["order by"] != nil {
		orderby = "ORDER BY " + query["order by"].(string)
		delete(query, "order by")
	}

	cols, findicies := s.analysisType(table, valueOf)
	qs, qp := s.buildMapQuery(query)

	querystring := fmt.Sprintf("SELECT %s FROM %s %s @@@ %s", cols, table, qs, orderby)
	return s.allocXP(valueOf, parname, pars, findicies, querystring, qp, limit)
}

func (s *Query) allocXP(v reflect.Value, parname string, pars []interface{}, findicies []int, querystring string, qp []interface{}, limit int) error {
	val := v //reflect.MakeSlice(v.Type(), 0, 1)
	for _, par := range pars {
		qssplit := strings.Split(querystring, "@@@")
		qs := qssplit[0] + " AND " + parname + "=?" + qssplit[1]
		cqp := append(qp, par)
		iter := s.Session.Query(qs, cqp...).Iter()
		ps := reflect.MakeSlice(val.Type(), 1, 1)
		t := val.Type().Elem().Elem()
		for limit > 0 {
			pnewele := reflect.New(t)
			data := make([]interface{}, 0, len(findicies))
			sfis := make(map[int]*[]byte)
			sfises := make(map[int]*[][]byte) // slice of struct fields indexs
			for _, i := range findicies {
				ele := buildPlaceHolder(i, pnewele.Elem(), sfis, sfises)
				data = append(data, ele)
			}

			if !iter.Scan(data...) {
				break
			}

			marshalTo(pnewele.Elem(), sfis, sfises)
			ps.Index(0).Set(pnewele)
			val = reflect.AppendSlice(val, ps)
			limit--
		}
		v.Set(val)
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

func containString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
