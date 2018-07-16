package cassandra

import (
	"strconv"
	"testing"
	"time"
)

func toString(i int) string {
	N := 10
	s := strconv.Itoa(i)
	if len(s) > N {
		panic("should increase N")
	}
	for i := 0; i < N; i++ {
		if len(s) == N {
			break
		}
		s = "0" + s
	}
	return s
}

func toString64(i int64) string {
	return toString(int(i))
}

var db ICassandra

type User struct {
	account, id, email string
	updated            int64
}

func TestMockView(t *testing.T) {
	db = NewCassandraFake(2, func(obj interface{}) []string {
		u := obj.(*User)
		return []string{toString(int(u.updated)), u.id, u.email}
	})
	now := time.Now().Unix()
	for i := 0; i < 100; i++ {
		u := &User{"1", toString(i % 30), toString(i) + "@thanh.cf", now + int64(i)%20}
		db.Upsert(u, u.account, u.id, u.email)
	}

	ti := time.Now().Unix() + 100
	oser := db.ListInView("1", false, 60, func(obj interface{}) bool {
		u := obj.(*User)
		return u.updated <= ti
	})

	if len(oser) != 60 {
		t.Fatalf("should be 60, got %d", len(oser))
	}
	last := oser[len(oser)-1].(*User)
	ti = last.updated
	oser = db.ListInView("1", false, 60, func(obj interface{}) bool {
		u := obj.(*User)
		if u.updated <= ti {
		}
		return u.updated <= ti
	})

	if len(oser) != 45 {
		t.Fatalf("should be 45, got %d", len(oser))
	}
}

func TestMock(t *testing.T) {
	db = NewCassandraFake(2, func(obj interface{}) []string {
		u := obj.(*User)
		return []string{toString(int(u.updated)), u.id, u.email}
	})

	now := time.Now().Unix()
	for i := 0; i < 100; i++ {
		u := &User{"1", toString(i % 30), toString(i) + "@thanh.cf", now + int64(i)%30}
		db.Upsert(u, u.account, u.id, u.email)
	}

	o := db.Read("1", toString(1), toString(1)+"@thanh.cf")
	u := o.(*User)
	if u.account != "1" || u.id != toString(1) || u.email != toString(1)+"@thanh.cf" {
		t.Fatal("should equal")
	}

	for i := 0; i < 100; i++ {
		db.Delete("1", toString(0), toString(i)+"@thanh.cf")
	}

	osers := db.List("1", true, 5, func(obj interface{}) bool {
		return true
	})

	for i, o := range osers {
		u := o.(*User)
		if u.account != "1" {
			t.Fatalf("should be 1, got %d", u.account)
		}
		if i == 4 {
			if u.id != toString(2) || u.email != toString(2)+"@thanh.cf" {
				t.Fatalf("should equal, got %s, %s", u.id, u.email)
			}
			continue
		}

		if u.id != toString(1) {
			t.Fatalf("should equal, got %s", u.id)
		}
	}
}

func TestUpdateView(t *testing.T) {
	db = NewCassandraFake(1, func(obj interface{}) []string {
		u := obj.(*User)
		return []string{u.email, u.id}
	})

	u := &User{"1", "sen", "sen@thanh.cf", 1}
	db.Upsert(u, u.account, u.id)
	u = &User{"1", "sen", "thanh@sen.cf", 2}
	db.Upsert(u, u.account, u.id)
	out := db.ListInView("1", true, 10, func(o interface{}) bool {
		return true
	})
	if len(out) != 1 {
		t.Fatalf("should be 1, got %d", len(out))
	}
}
