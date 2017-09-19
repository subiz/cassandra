package cassandra

import (
	json "github.com/pquerna/ffjson/ffjson"
	"github.com/gocql/gocql"
	"reflect"
	"fmt"
	"strings"
	"github.com/cenkalti/backoff"
	"time"
	"errors"
	"github.com/golang/protobuf/proto"
)

type Query interface {
	GetSession() *gocql.Session
	Delete(table string, query interface{}) error
	Read(table string, p interface{}, query interface{}) error
	Upsert(table string, p interface{}) error
	CreateKeyspace(seeds []string, keyspace string, repfactor int) error
	List(table string, p interface{}, query map[string]interface{}, limit int) error
	ReadBatch(table string, p interface{}, query map[string]interface{}) error
}

func NewQuery() *SQuery {
	return &SQuery{}
}

type SQuery struct {
	session *gocql.Session
}

func (s *SQuery) GetSession() *gocql.Session {
	return s.session
}

func (s *SQuery) Delete(table string, query interface{}) error {
	var m map[string]interface{}
	b, err := json.Marshal(query)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	qs, qp, err := s.buildQuery(query)
	if err != nil {
		return err
	}
	if qs == "" {
		return nil
	}
	//	qs = " WHERE " + qs
	querystring := fmt.Sprintf("DELETE FROM %s %s", table , qs)
	return s.session.Query(querystring, qp...).Exec()
}

func (me *SQuery) CreateKeyspace(seeds []string, keyspace string, repfactor int) (err error) {
	cluster := gocql.NewCluster(seeds...)
	cluster.Timeout = 10 * time.Second
	cluster.Keyspace = "system"
	ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
	var defsession *gocql.Session
	for range ticker.C {
		defsession, err = cluster.CreateSession()
		if err == nil {
			ticker.Stop()
			break
		}
		//common.Log(err, "will retry...")
	}
	if err != nil {
		return err
	}
	defer func() {
		defsession.Close()
		cluster.Keyspace = keyspace
		me.session, err = cluster.CreateSession()
	}()

	return defsession.Query(fmt.Sprintf(
	`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class': 'SimpleStrategy',
		'replication_factor': %d
	}`, keyspace, repfactor)).Exec()
}

func (s *SQuery) Upsert(table string, p interface{}) error {
	columns := make([]string, 0)
	phs := make([]string, 0) // place holders
	data := make([]interface{}, 0)
	valueOf := reflect.ValueOf(p).Elem()
	typeOf := reflect.TypeOf(p).Elem()
	for i := 0; i < valueOf.NumField(); i++ {
		vf := valueOf.Field(i)
		tf := typeOf.Field(i)
		jsonname := strings.Split(tf.Tag.Get("json"), ",")[0]
		if jsonname == "-" {
			continue
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
	return s.session.Query(querystring, data...).Exec()
}

func (s *SQuery) buildQuery(query interface{}) (string, []interface{}, error) {
	var m map[string]interface{}
	b, err := json.Marshal(query)
	if err != nil {
		return "", nil, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return "", nil, err
	}

	q := make([]string, 0, len(m)) // query
	qp := make([]interface{}, 0, len(m))
	for k, v := range m {
		q = append(q, k + "=?")
		qp = append(qp, v)
	}
	qs := strings.Join(q, " AND ")
	if qs == "" {
		return "", nil, nil
	}
	return " WHERE " + qs, qp, nil
}

func (s *SQuery) Read(table string, p interface{}, query interface{}) error {
	valueOf := reflect.New(reflect.SliceOf(reflect.TypeOf(p))).Elem()
	cols, findicies := s.analysisType(valueOf)
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

func (s *SQuery) buildMapQuery(query map[string]interface{}) (string, []interface{}) {
	q := make([]string, 0, len(query)) // query
	qp := make([]interface{}, 0, len(query))
	for k, v := range query {
		q = append(q, k + "?")
		qp = append(qp, v)
	}
	qs := strings.Join(q, " AND ")
	if qs == "" {
		return "", nil
	}
	return " WHERE " + qs, qp
}

// p is pointer to array of pointer
func (s *SQuery) analysisType(p reflect.Value) (cols string, findices []int) {
	columns := make([]string, 0)
	eleTypeOf := p.Type().Elem().Elem()
	validC := make([]int, 0)
	for i := 0; i < eleTypeOf.NumField(); i++ {
		tf := eleTypeOf.Field(i)
		jsonname := strings.Split(tf.Tag.Get("json"), ",")[0]
		if jsonname == "-" {
			continue
		}
		validC = append(validC, i)
		columns = append(columns, jsonname)
	}
	return strings.Join(columns, ","), validC
}

func (s *SQuery) List(table string, p interface{}, query map[string]interface{}, limit int) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return errors.New("cassandra reflect error: p must be a pointer to array")
	}
	valueOf := reflect.ValueOf(p).Elem() // a slice

	cols, findicies := s.analysisType(valueOf)
	qs, qp := s.buildMapQuery(query)
	querystring := fmt.Sprintf("SELECT %s FROM %s %s LIMIT %v", cols, table, qs, limit)
	return s.alloc(valueOf, findicies, querystring, qp)
}

func (s *SQuery) buildBatchQuery(query map[string]interface{}) (string, []interface{}) {
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
			phs := " (" +  strings.Join(ph, ",") + ")"
			q = append(q, k + phs)
		} else {
			q = append(q, k + "?")
			qp = append(qp, v)
		}
	}
	qs := strings.Join(q, " AND ")
	if qs == "" {
		return "", nil
	}
	return " WHERE " + qs, qp
}

func (s *SQuery) alloc(v reflect.Value, findicies []int, querystring string, qp []interface{}) error {
	val := v
	iter := s.session.Query(querystring, qp...).Iter()
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

func (s *SQuery) ReadBatch(table string, p interface{}, query map[string]interface{}) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return errors.New("cassandra reflect error: p must be a pointer to array")
	}
	valueOf := reflect.ValueOf(p).Elem() // a slice
	cols, findicies := s.analysisType(valueOf)
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
