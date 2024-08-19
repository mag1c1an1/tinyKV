package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	m := []storage.Modify{
		{
			storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	}
	err := server.storage.Write(req.Context, m)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	m := []storage.Modify{
		{
			storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	}
	err := server.storage.Write(req.Context, m)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	limit := req.GetLimit()

	for iter.Seek(req.GetStartKey()); iter.Valid() && limit > 0; iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			resp.Error = err.Error()
			resp.Kvs = nil
			return resp, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: val})
		limit -= 1
	}
	return resp, nil
}
