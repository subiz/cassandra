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
	var m map[string]interface{}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

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

func (s *SQuery)  buildQuery(query interface{}) (string, []interface{}, error) {
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
	typeOf := reflect.TypeOf(p).Elem()
	valueOf := reflect.ValueOf(p).Elem()
	columns := make([]string, 0)
	data := make([]interface{}, 0)

	sfis := make(map[int]*[]byte) // struct fields indexs
	sfises := make(map[int]*[][]byte) // slice of struct fields indexs
	for i := 0; i < valueOf.NumField(); i++ {
		vf := valueOf.Field(i)
		tf := typeOf.Field(i)
		jsonname := strings.Split(tf.Tag.Get("json"), ",")[0]
		if jsonname == "-" {
			continue
		}
		columns = append(columns, jsonname)
		if vf.Type().Kind() == reflect.Slice && vf.Type().Elem().Kind() == reflect.Ptr {
			var bs [][]byte
			sfises[i] = &bs
			data = append(data, &bs)
		} else if vf.Type().Kind() == reflect.Ptr && vf.Type().Elem().Kind() == reflect.Struct {
			var b []byte
			sfis[i] = &b
			data = append(data, &b)
		} else {
			data = append(data, vf.Addr().Interface())
		}
	}

	qs, qp, err := s.buildQuery(query)
	if err != nil {
		return err
	}
	if qs == "" {
		return nil
	}
	querystring := fmt.Sprintf("SELECT %s FROM %s %s LIMIT 1", strings.Join(columns, ","), table, qs)
	err = s.session.Query(querystring, qp...).Scan(data...)
	if err != nil {
		return err
	}

	for k, v := range sfis {
		if v == nil {
			continue
		}
		vf := valueOf.Field(k)
		pnewele := reflect.New(vf.Type().Elem())
		err := proto.Unmarshal(*v, pnewele.Interface().(proto.Message))
		if err != nil {
			return err
		}
		vf.Set(pnewele)
	}

	for k, v := range sfises {
		if v == nil {
			continue
		}
		vf := valueOf.Field(k)

		dest := reflect.MakeSlice(vf.Type(), 0, 0)
		ss := reflect.MakeSlice(vf.Type(), 1, 1)
		for _, b := range *v {
			pnewele := reflect.New(vf.Type().Elem().Elem())
			proto.Unmarshal(b, pnewele.Interface().(proto.Message))
			ss.Index(0).Set(pnewele)
			dest = reflect.AppendSlice(dest, ss)
		}
		vf.Set(dest)
	}
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

func (s *SQuery) analysisType(p interface{}) (cols string, findices []int) {
	columns := make([]string, 0)
	eleTypeOf := reflect.TypeOf(p).Elem().Elem().Elem()
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

	typeOf := reflect.TypeOf(p).Elem()
	valueOf := reflect.ValueOf(p).Elem() // a slice

	cols, findicies := s.analysisType(p)
	qs, qp := s.buildMapQuery(query)
	querystring := fmt.Sprintf("SELECT %s FROM %s %s LIMIT %v", cols, table, qs, limit)
	iter := s.session.Query(querystring, qp...).Iter()
	ps := reflect.MakeSlice(typeOf, 1, 1)
	t := typeOf.Elem().Elem()
	for {
		pnewele := reflect.New(t)
		data := make([]interface{}, 0, len(findicies))
		sfis := make(map[int]*[]byte)
		sfises := make(map[int]*[][]byte) // slice of struct fields indexs
		for _, i := range findicies {
			vf := pnewele.Elem().Field(i)

			if vf.Type().Kind() == reflect.Slice && vf.Type().Elem().Kind() == reflect.Ptr {
				var bs [][]byte
				sfises[i] = &bs
				data = append(data, &bs)
			} else if vf.Type().Kind() == reflect.Ptr && vf.Type().Elem().Kind() == reflect.Struct {
				var b []byte
				sfis[i] = &b
				data = append(data, &b)
			} else {
				data = append(data, vf.Addr().Interface())
			}
		}
		if !iter.Scan(data...) {
			break
		}

		for k, v := range sfis {
			if v == nil {
				continue
			}
			tf := valueOf.Type().Elem().Elem().Field(k)
			pf := reflect.New(tf.Type.Elem())
			err := proto.Unmarshal(*v, pf.Interface().(proto.Message))
			if err != nil {
				return err
			}
			pnewele.Elem().Field(k).Set(pf)
		}

		for k, v := range sfises {
			if v == nil {
				continue
			}
			vf := pnewele.Elem().Field(k)

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

		ps.Index(0).Set(pnewele)
		valueOf = reflect.AppendSlice(valueOf, ps)
	}
	reflect.ValueOf(p).Elem().Set(valueOf)
	return iter.Close()
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

func (s *SQuery) ReadBatch(table string, p interface{}, query map[string]interface{}) error {
	if reflect.TypeOf(p).Kind() != reflect.Ptr {
		return errors.New("cassandra reflect error: p must be a pointer to array")
	}

	typeOf := reflect.TypeOf(p).Elem()
	valueOf := reflect.ValueOf(p).Elem() // a slice

	cols, findicies := s.analysisType(p)
	qs, qp := s.buildBatchQuery(query)
	querystring := fmt.Sprintf("SELECT %s FROM %s %s", cols, table, qs)
	iter := s.session.Query(querystring, qp...).Iter()
	ps := reflect.MakeSlice(typeOf, 1, 1)
	t := typeOf.Elem().Elem()
	for {
		pnewele := reflect.New(t)
		data := make([]interface{}, 0, len(findicies))
		sfis := make(map[int]*[]byte)
		sfises := make(map[int]*[][]byte) // slice of struct fields indexs
		for _, i := range findicies {
			vf := pnewele.Elem().Field(i)
			if vf.Type().Kind() == reflect.Slice && vf.Type().Elem().Kind() == reflect.Ptr {
				var bs [][]byte
				sfises[i] = &bs
				data = append(data, &bs)
			} else if vf.Type().Kind() == reflect.Ptr && vf.Type().Elem().Kind() == reflect.Struct {
				var b []byte
				sfis[i] = &b
				data = append(data, &b)
			} else {
				data = append(data, vf.Addr().Interface())
			}
		}
		if !iter.Scan(data...) {
			break
		}

			for k, v := range sfis {
			if v == nil {
				continue
			}
			tf := valueOf.Type().Elem().Elem().Field(k)
			pf := reflect.New(tf.Type.Elem())
			err := proto.Unmarshal(*v, pf.Interface().(proto.Message))
			if err != nil {
				return err
			}
			pnewele.Elem().Field(k).Set(pf)
		}

		for k, v := range sfises {
			if v == nil {
				continue
			}
			vf := pnewele.Elem().Field(k)

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

		ps.Index(0).Set(pnewele)
		valueOf = reflect.AppendSlice(valueOf, ps)
	}
	reflect.ValueOf(p).Elem().Set(valueOf)
	return iter.Close()
}
