// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstable logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, _ := storage.InitialState()
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if len(l.entries) == 0 {
		return
	}
	truncatedIndex, _ := l.storage.FirstIndex()
	firstIndex := l.FirstIndex()
	if truncatedIndex > firstIndex {
		l.entries = l.entries[truncatedIndex-firstIndex:]
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	l.maybeCompact()
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		if l.stabled-firstIndex+1 < 0 || l.stabled-firstIndex+1 > uint64(len(l.entries)) {
			return nil
		}
		return l.entries[l.stabled-firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()
		if l.committed-firstIndex+1 < 0 || l.applied-firstIndex+1 > lastIndex {
			return nil
		}
		if l.applied-firstIndex+1 >= 0 && l.committed-firstIndex+1 <= uint64(len(l.entries)) {
			start := l.applied - firstIndex + 1
			end := l.committed - firstIndex + 1
			return l.entries[start:end]
		}
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.stabled
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		// this is log's truncated index
		i, _ := l.storage.FirstIndex()
		return i - 1
	}
	return l.entries[0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		offset := l.FirstIndex()
		if i >= offset {
			index := i - offset
			if index >= uint64(len(l.entries)) {
				return 0, ErrUnavailable
			}
			return l.entries[index].Term, nil
		}
	}
	term, err := l.storage.Term(i)

	if errors.Is(err, ErrUnavailable) && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			// can return it
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if i < l.pendingSnapshot.Metadata.Index {
			// don't have it now
			err = ErrCompacted
		}
	}

	return term, err
}
func (l *RaftLog) appendEntries(entries ...pb.Entry) {
	l.entries = append(l.entries, entries...)
}

// Entries returns entries in [lo, hi)
func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	firstIndex := l.FirstIndex()
	if lo >= firstIndex && hi-firstIndex <= uint64(len(l.entries)) {
		return l.entries[lo-firstIndex : hi-firstIndex]
	}
	entries, err := l.storage.Entries(lo, hi)
	if err != nil {
		panic(err)
	}
	return entries
}

func (l *RaftLog) removeEntriesFrom(index uint64) {
	// maybe remove stabled entries
	l.stabled = min(index-1, l.stabled)
	firstIndex := l.FirstIndex()
	if index-firstIndex >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index-firstIndex]
}
