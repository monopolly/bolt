package bolt

import (
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

type Tx = bbolt.Tx

type DB struct {
	db *bbolt.DB
}

func New(path string, buckets ...string) (a *DB, err error) {
	a = new(DB)
	db, err := bbolt.Open(path, os.ModePerm, bbolt.DefaultOptions)
	if err != nil {
		return
	}
	a.db = db
	a.Buckets(buckets...)
	return
}

func (a *DB) Close() {
	a.db.Close()
}

//helper to create buckets
func (a *DB) Buckets(buckets ...string) {
	if a == nil {
		return
	}
	a.db.Update(func(i *Tx) (err error) {
		for _, x := range buckets {
			i.CreateBucketIfNotExists([]byte(x))
		}
		return
	})
}

//helper to create buckets
func (a *DB) Next(counter string) (id int) {
	a.db.Update(func(i *Tx) (err error) {
		b, err := i.CreateBucketIfNotExists([]byte(counter))
		if err != nil {
			return
		}
		ids, _ := b.NextSequence()
		id = int(ids)
		return
	})
	return
}

//helper to create buckets
func (a *DB) Next64(counter string) (id uint64) {
	a.db.Update(func(i *Tx) (err error) {
		b, err := i.CreateBucketIfNotExists([]byte(counter))
		if err != nil {
			return
		}
		id, _ = b.NextSequence()
		return
	})
	return
}

func (a *DB) Select(bucket []byte, limit, offset int, handler func(k, v []byte)) {
	a.db.View(func(t *Tx) (err error) {
		bk := t.Bucket(bucket)
		c := bk.Cursor()
		var count, served int
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			count++
			if offset > 0 && count < offset {
				continue
			}

			if limit > 0 && served >= limit {
				return
			}

			handler(k, v)
			served++
		}
		return
	})
}

//helper to create buckets
func (a *DB) HasBucket(bucket string) (has bool) {
	if a.db == nil {
		return
	}
	a.db.View(func(i *Tx) (err error) {
		has = (i.Bucket([]byte(bucket)) != nil)
		return
	})
	return
}

func (a *DB) Get(bucket, id []byte) (value []byte) {
	a.db.View(func(t *Tx) (err error) {
		b := t.Bucket(bucket)
		if b == nil {
			return
		}
		value = b.Get(id)
		return
	})
	return
}

func (a *DB) Has(bucket, id []byte) (has bool) {
	a.db.View(func(t *Tx) (err error) {
		b := t.Bucket(bucket)
		if b == nil {
			return
		}
		has = len(b.Get(id)) > 0
		return
	})
	return
}

// get ids
func (a *DB) List(bucket string, h func(value []byte), id ...string) {
	a.db.View(func(t *Tx) (err error) {
		b := t.Bucket([]byte(bucket))
		if b == nil {
			return
		}
		for _, x := range id {
			h(b.Get([]byte(x)))
		}
		return
	})
	return
}

// get ids
func (a *DB) ListB(bucket []byte, h func(value []byte), id ...[]byte) {
	a.db.View(func(t *Tx) (err error) {
		b := t.Bucket(bucket)
		if b == nil {
			return
		}
		for _, x := range id {
			h(b.Get(x))
		}
		return
	})
	return
}

func (a *DB) Delete(bucket, id []byte) {
	a.db.Update(func(t *Tx) (err error) {
		b := t.Bucket(bucket)
		b.Delete(id)
		return
	})
	return
}

func (a *DB) Deletes(bucket, id string) {
	a.db.Update(func(t *Tx) (err error) {
		b := t.Bucket([]byte(bucket))
		b.Delete([]byte(id))
		return
	})
	return
}

func (a *DB) ResetBucket(bucket string) {
	a.db.Update(func(t *Tx) (err error) {
		t.DeleteBucket([]byte(bucket))
		t.CreateBucketIfNotExists([]byte(bucket))
		return
	})
	return
}

func (a *DB) Add(bucket, k, v []byte) {
	a.db.Update(func(t *Tx) (err error) {
		b := t.Bucket(bucket)
		b.Put(k, v)
		return
	})
	return
}

func (a *DB) Int(bucket []byte, k string, v ...int) (r int) {
	switch len(v) > 0 {
	case true:
		p := a.Get(bucket, []byte(k))
		if p == nil {
			return
		}
		return bi(p)

	default:
		a.Add(bucket, []byte(k), ib(v[0]))
	}
	return
}

func (a *DB) Int64(bucket []byte, k string, v ...int64) (r int64) {
	switch len(v) > 0 {
	case true:
		p := a.Get(bucket, []byte(k))
		if p == nil {
			return
		}
		return bi64(p)

	default:
		a.Add(bucket, []byte(k), ib64(v[0]))
	}
	return
}

func (a *DB) Uint64(bucket []byte, k string, v ...uint64) (r uint64) {
	switch len(v) > 0 {
	case true:
		p := a.Get(bucket, []byte(k))
		if p == nil {
			return
		}
		return bu(p)

	default:
		a.Add(bucket, []byte(k), ub(v[0]))
	}
	return
}

func (a *DB) String(bucket []byte, k string, v ...string) (r string) {
	switch len(v) > 0 {
	case true:
		p := a.Get(bucket, []byte(k))
		if p == nil {
			return
		}
		return string(p)
	default:
		a.Add(bucket, []byte(k), []byte(v[0]))
	}
	return
}

func (a *DB) Bool(bucket []byte, k string, v ...bool) (r bool) {
	switch len(v) > 0 {
	case true:
		p := a.Get(bucket, []byte(k))
		if p == nil {
			return
		}
		return string(p) == "1"
	default:
		a.Add(bucket, []byte(k), []byte("1"))
	}
	return
}

func (a *DB) Count(bucket string) (count int) {
	a.db.View(func(t *Tx) (err error) {
		b := t.Bucket([]byte(bucket))
		if err != nil {
			return
		}
		b.ForEach(func(k, v []byte) (err error) {
			if k == nil {
				return
			}
			count++
			return
		})
		return
	})
	return
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (a *DB) Iterate(bucket string, f func(k, v []byte) (stop bool)) {
	a.db.View(func(tx *Tx) (err error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return
		}
		b.ForEach(func(k, v []byte) (err error) {
			if k != nil && v != nil {
				if f(k, v) {
					return fmt.Errorf("stop")
				}
			}
			return
		})
		return
	})
}

func (a *DB) IterateCount(bucket string, count int, f func(k, v []byte)) {
	if count == 0 {
		return
	}
	var until int
	a.db.View(func(tx *Tx) (err error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return
		}
		b.ForEach(func(k, v []byte) (err error) {
			if k != nil && v != nil {
				until++
				f(k, v)
				if until == count {
					return fmt.Errorf("stop")
				}
			}
			return
		})
		return
	})
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (a *DB) Iterates(bucket string, f func(k, v []byte)) {
	a.db.View(func(tx *Tx) (err error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return
		}
		b.ForEach(func(k, v []byte) (err error) {
			if k != nil && v != nil {
				f(k, v)
			}
			return
		})
		return
	})
}

func (a *DB) IteratesB(bucket []byte, f func(k, v []byte)) {
	a.db.View(func(tx *Tx) (err error) {
		b := tx.Bucket(bucket)
		if b == nil {
			return
		}
		b.ForEach(func(k, v []byte) (err error) {
			if k != nil && v != nil {
				f(k, v)
			}
			return
		})
		return
	})
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (a *DB) All(bucket string, f func(k, v []byte)) {
	a.db.View(func(tx *Tx) (err error) {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return
		}
		b.ForEach(func(k, v []byte) (err error) {
			if k != nil && v != nil {
				f(k, v)
			}
			return
		})
		return
	})
}
