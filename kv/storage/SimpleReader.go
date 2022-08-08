package storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type SimpleReader struct {
	txn *badger.Txn
}

func NewSimpleReader(txn *badger.Txn) *SimpleReader {
	return &SimpleReader{
		txn: txn,
	}
}

func (s *SimpleReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *SimpleReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

// Close Don’t forget to call Discard() for badger.Txn and close all iterators before discarding.
func (s *SimpleReader) Close() {
	s.txn.Discard()
}
