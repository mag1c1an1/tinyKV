package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Db     *badger.DB
	Config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		Db:     nil,
		Config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	options := badger.DefaultOptions
	options.Dir = s.Config.DBPath
	options.ValueDir = s.Config.DBPath
	db, err := badger.Open(options)
	if err != nil {
		return err
	}
	s.Db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.Db.Close()
}

type ReaderImpl struct {
	Txn *badger.Txn
}

func (reader ReaderImpl) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(reader.Txn, cf, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return value, nil
}

func (reader ReaderImpl) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.Txn)
}

func (reader ReaderImpl) Close() {
	reader.Txn.Discard()
}

func NewReaderImpl(db *badger.DB) ReaderImpl {
	txn := db.NewTransaction(false)
	return ReaderImpl{
		Txn: txn,
	}
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return NewReaderImpl(s.Db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		cf := m.Cf()
		key := m.Key()
		val := m.Value()
		if val == nil {
			wb.DeleteCF(cf, key)
		} else {
			wb.SetCF(cf, key, val)
		}

	}
	return wb.WriteToDB(s.Db)
}
