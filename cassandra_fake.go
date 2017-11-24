package cassandra

import (
	"strings"
	"sort"
)

type MyDB struct {
	keys [][]string
	values []interface{}
}
func NewMyDB() *MyDB {
	return &MyDB{
		keys: make([][]string, 0),
		values: make([]interface{}, 0),
	}
}

func (m MyDB) SortKey(asc bool) [][]string {
	deli := "\u0007"
	keys := make([]string, 0, len(m.keys))
	for _, k := range m.keys {
		keys = append(keys, strings.Join(k, deli))
	}
	sort.Strings(keys)
	out := make([][]string, 0)

	if asc {
		for i := 0; i < len(keys); i++ {
			out = append(out, strings.Split(keys[i], deli))
		}
	} else {
		for i := len(keys) - 1; i >= 0; i-- {
			out = append(out, strings.Split(keys[i], deli))
		}
	}
	return out
}

func (m MyDB) Keys() [][]string {
	return m.keys
}

func (m *MyDB) Delete(keys ...string) {
	for t, k := range m.keys {
		found := true
		for i := range k {
			if k[i] != keys[i] {
				found = false
				break
			}
		}

		if found {
			m.values = append(m.values[:t], m.values[t+1:]...)
			m.keys = append(m.keys[:t], m.keys[t+1:]...)
			return
		}
	}
}

func (m *MyDB) Set(obj interface{}, keys ...string) {
	for t, k := range m.keys {
		found := true
		for i := range k {
			if k[i] != keys[i] {
				found = false
				break
			}
		}

		if found {
			m.values[t] = obj
			return
		}
	}

	m.keys = append(m.keys, keys)
	m.values = append(m.values, obj)
}

func (m MyDB) Get(keys ...string) interface{} {
	for t, k := range m.keys {
		found := true
		for i := range k {
			if k[i] != keys[i] {
				found = false
				break
			}
		}

		if found {
			return m.values[t]
		}
	}
	return nil
}

type CassandraFake struct {
	viewmapf func(obj interface{})[]string
	nclustering int
	db map[string]*MyDB
	view map[string]*MyDB
}

func NewCassandraFake(nclustering int, viewmapf func(obj interface{})[]string) *CassandraFake {
	f := &CassandraFake{}
	f.nclustering = nclustering
	f.viewmapf = viewmapf
	f.db = make(map[string]*MyDB)
	f.view = make(map[string]*MyDB)
	return f
}

func (f CassandraFake) Read(partition string, clustering ...string) interface{} {
	db, ok := f.db[partition]
	if !ok {
		return nil
	}
	return db.Get(clustering...)
}

func (f *CassandraFake) Upsert(obj interface{}, partition string, clustering ...string) {
	db, ok := f.db[partition]
	if !ok {
		db = NewMyDB()
		f.db[partition] = db
		f.view[partition] = NewMyDB()
	}

	view := f.view[partition]

	// remove old object if exists
	o := db.Get(clustering...)
	if o != nil {
		view.Delete(f.viewmapf(o)...)
	}

	view.Set(obj, f.viewmapf(obj)...)
	db.Set(obj, clustering...)
}

func (f CassandraFake) Delete(partition string, clustering ...string) {
	db, ok := f.db[partition]
	if !ok {
		return
	}
	view := f.view[partition]
	obj := db.Get(clustering...)
	if obj == nil {
		return
	}
	view.Delete(f.viewmapf(obj)...)
	db.Delete(clustering...)
}

func (f CassandraFake) List(partition string, asc bool, limit int, condf func(obj interface{}) bool) []interface{} {
	db, ok := f.db[partition]
	if !ok {
		return nil
	}

	out := make([]interface{}, 0)
	keys := db.SortKey(asc)
	for _, k := range keys {
		o := db.Get(k...)
		if !condf(o) {
			continue
		}
		out = append(out, o)
		if len(out) == limit {
			break
		}
	}
	return out
}

func (f CassandraFake) ListInView(partition string, asc bool, limit int, condf func(obj interface{}) bool) []interface{} {
	view, ok := f.view[partition]
	if !ok {
		return nil
	}

	out := make([]interface{}, 0)
	keys := view.SortKey(asc)
	for _, k := range keys {
		o := view.Get(k...)
		if !condf(o) {
			continue
		}
		out = append(out, o)
		if len(out) == limit {
			break
		}
	}
	return out
}
