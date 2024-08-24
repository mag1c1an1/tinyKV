// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (s storeSlice) Len() int {
	return len(s)
}

func (s storeSlice) Less(i, j int) bool {
	return s[i].GetRegionSize() < s[j].GetRegionSize()
}

func (s storeSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	suitableStores := make(storeSlice, 0)
	for _, s := range cluster.GetStores() {
		if s.IsUp() && s.DownTime() < maxStoreDownTime {
			//log.Infof("suitable store %d, size %d\n", s.GetID(), s.GetRegionSize())
			suitableStores = append(suitableStores, s)
		}
	}
	sort.Sort(sort.Reverse(suitableStores))
	var originStore *core.StoreInfo = nil
	var movedRegion *core.RegionInfo = nil
	// find origin region
	for _, s := range suitableStores {
		cluster.GetPendingRegionsWithLock(s.GetID(), func(c core.RegionsContainer) {
			movedRegion = c.RandomRegion(nil, nil)
			originStore = s
		})
		if movedRegion != nil {
			break
		}
		cluster.GetFollowersWithLock(s.GetID(), func(c core.RegionsContainer) {
			movedRegion = c.RandomRegion(nil, nil)
			originStore = s
		})
		if movedRegion != nil {
			break
		}
		cluster.GetLeadersWithLock(s.GetID(), func(c core.RegionsContainer) {
			movedRegion = c.RandomRegion(nil, nil)
			originStore = s
		})
		if movedRegion != nil {
			break
		}
	}
	if movedRegion == nil {
		log.Warn("can't find origin store to move")
		return nil
	}

	storeIds := movedRegion.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		log.Warn("moved region %d not  max replica\n", movedRegion.GetID())
		return nil
	}

	var targetStore *core.StoreInfo = nil
	for j := len(suitableStores) - 1; j >= 0; j-- {
		if suitableStores[j].GetID() == originStore.GetID() {
			break
		}
		// region not on that store
		if _, ok := storeIds[suitableStores[j].GetID()]; !ok {
			targetStore = suitableStores[j]
			break
		}
	}
	if targetStore == nil {
		log.Warn("can't find target store")
		return nil
	}
	if originStore.GetRegionSize()-targetStore.GetRegionSize() < 2*movedRegion.GetApproximateSize() {
		log.Warn("difference between %d and %d is not enough\n", originStore.GetID(), targetStore.GetID())
		return nil
	}
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		log.Errorf("alloc new peer on store %d err %+v\n", targetStore.GetID(), err)
		return nil
	}
	op, err := operator.CreateMovePeerOperator(
		fmt.Sprintf(
			"region %d oved from store %d to store %d\n",
			movedRegion.GetID(),
			originStore.GetID(),
			targetStore.GetID(),
		),
		cluster,
		movedRegion,
		operator.OpBalance,
		originStore.GetID(),
		targetStore.GetID(),
		newPeer.GetId())

	if err != nil {
		log.Errorf("create move peer operator failed, err %+v\n", err)
		return nil
	}
	return op
}
