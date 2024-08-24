package raftstore

import (
	goerrors "errors"
	"fmt"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/raft"

	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
	"reflect"
	"time"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	ready := d.RaftGroup.Ready()
	// save unstable entries and RaftLocalState
	result, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic(err)
	}
	// update region info
	if result != nil && !reflect.DeepEqual(result.PrevRegion, result.Region) {
		d.peerStorage.SetRegion(result.Region)
		// TODO
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[result.Region.GetId()] = result.Region
		//storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
		storeMeta.Unlock()
	}

	if len(ready.Messages) > 0 {
		d.Send(d.ctx.trans, ready.Messages)
	}

	// apply committed entries
	if len(ready.CommittedEntries) > 0 {
		writeBatch := &engine_util.WriteBatch{}
		for _, entry := range ready.CommittedEntries {
			writeBatch = d.applyEntry(entry, writeBatch)
			if d.stopped {
				return
			}
		}

		d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
		err := writeBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
			return
		}
		writeBatch.MustWriteToDB(d.peerStorage.Engines.Kv)
	}
	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	// FIXME
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	var errEpochNotMatching *util.ErrEpochNotMatch
	if goerrors.As(err, &errEpochNotMatching) {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.Raft.GetLeadTransferee() != raft.None {
		err := fmt.Sprintf("%s is now transfer leader to %d\n", d.Tag, d.RaftGroup.Raft.GetLeadTransferee())
		errResp := ErrResp(errors.New(err))
		if msg.AdminRequest != nil {
			errResp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: msg.AdminRequest.CmdType}
		}
		cb.Done(errResp)
	}
	if msg.AdminRequest != nil {
		d.proposeAdminCommand(msg, cb)
	} else {
		d.proposeBasicRaftCommand(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// no need to do propose
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// duplicate conf change, wait
		if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
			log.Infof("%s already pending conf change index %d", d.Tag, d.RaftGroup.Raft.PendingConfIndex)
			err := fmt.Sprintf("%s already pending conf change index %d\n", d.Tag, d.RaftGroup.Raft.PendingConfIndex)
			cb.Done(ErrResp(errors.New(err)))
			return
		}
		// FIXME
		if req.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && d.IsLeader() &&
			len(d.Region().Peers) == 2 && req.ChangePeer.Peer.Id == d.PeerId() {
			log.Infof("%s return corner case\n", d.Tag)
			for _, p := range d.Region().Peers {
				if p.Id != req.ChangePeer.Peer.Id {
					d.RaftGroup.TransferLeader(p.Id)
					break
				}
			}
			err := fmt.Sprintf("%s return corner case\n", d.Tag)
			cb.Done(ErrResp(errors.New(err)))
			return
		}

		log.Infof("%s propose new conf change %+v at index %d", d.Tag, msg, d.nextProposalIndex())
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		ctx, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId:     req.ChangePeer.Peer.Id,
			Context:    ctx,
		})
		if err != nil {
			panic(err)
			return
		}
	case raft_cmdpb.AdminCmdType_Split:
		if err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region()); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		err = d.RaftGroup.Propose(data)
		if err != nil {
			panic(err)
			return
		}
	default:
		panic("wrong admin cmd")
	}
}

func (d *peerMsgHandler) proposeBasicRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// only one req per call from high-level get/put/delete
	req := msg.Requests[0]
	key := getRequestedKey(req)
	// check
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}

	d.proposals = append(d.proposals, &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	})

	err = d.RaftGroup.Propose(data)
	if err != nil {
		panic(err)
		return
	}
}

func getRequestedKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	}
	return nil
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot, or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise, a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be updated
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) applyEntry(entry eraftpb.Entry, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	// nop entry
	if entry.Data == nil {
		return wb
	}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := &eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		return d.applyConfChange(entry, cc, wb)
	}
	req := &raft_cmdpb.RaftCmdRequest{}
	err := req.Unmarshal(entry.Data)
	if err != nil {
		println(err)
		panic(err)
	}
	if req.AdminRequest != nil {
		return d.applyAdminRequest(entry, req, wb)
	}
	if len(req.Requests) > 0 {
		return d.applyBasicRequest(entry, req, wb)
	}
	return nil
}

func (d *peerMsgHandler) applyAdminRequest(entry eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.AdminRequest
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compact := req.GetCompactLog()
		applyState := d.peerStorage.applyState
		if compact.CompactIndex > applyState.TruncatedState.Index {
			// update truncated state
			applyState.TruncatedState.Index = compact.CompactIndex
			applyState.TruncatedState.Term = compact.CompactTerm
			err := wb.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
			if err != nil {
				panic(err)
			}
			d.ScheduleCompactLog(applyState.TruncatedState.Index)
		}
	case raft_cmdpb.AdminCmdType_Split:
		region := d.Region()
		var err *util.ErrEpochNotMatch
		if goerrors.As(util.CheckRegionEpoch(msg, region, true), &err) {
			sibling := d.findSiblingRegion()
			if sibling != nil {
				err.Regions = append(err.Regions, sibling)
			}
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
		splitReq := req.GetSplit()
		if err := util.CheckKeyInRegion(splitReq.SplitKey, region); err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
		//log.Infof("%s begin split region %d [%s, %s]\n", d.Tag, region.Id, region.StartKey, region.EndKey)
		newPeers := make([]*metapb.Peer, 0)
		for i, p := range region.Peers {
			newPeers = append(newPeers, &metapb.Peer{
				Id:      splitReq.NewPeerIds[i],
				StoreId: p.StoreId,
			})
		}
		newRegion := &metapb.Region{
			Id:       splitReq.NewRegionId,
			StartKey: region.StartKey,
			EndKey:   splitReq.SplitKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: region.RegionEpoch.ConfVer,
				Version: region.RegionEpoch.Version + 1,
			},
			Peers: newPeers,
		}
		region.RegionEpoch.Version += 1
		region.StartKey = splitReq.SplitKey

		meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
		meta.WriteRegionState(wb, newRegion, rspb.PeerState_Normal)
		newPeer, cErr := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if cErr != nil {
			panic(err)
		}

		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()

		storeMeta.setRegion(region, d.peer)
		storeMeta.setRegion(newRegion, newPeer)
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})

		storeMeta.Unlock()

		wb.MustWriteToDB(d.peerStorage.Engines.Kv)
		wb = &engine_util.WriteBatch{}

		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)

		d.ctx.router.register(newPeer)
		d.ctx.router.send(newRegion.Id, message.NewMsg(message.MsgTypeStart, nil))
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
		//log.Infof("%s finish split region %d to region %d [%s, %s] and new region %d [%s, %s]\n", d.Tag,
		//	region.Id, region.Id, region.StartKey, region.EndKey, newRegion.Id, newRegion.StartKey, newRegion.EndKey)
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split: &raft_cmdpb.SplitResponse{
						Regions: []*metapb.Region{region, newRegion},
					},
				},
			})
		})
	}
	return wb
}

func (d *peerMsgHandler) applyBasicRequest(entry eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.Requests[0]
	key := getRequestedKey(req)
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return wb
		}
	}

	// proposals only on the peer that was leader when proposed
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		wb.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	case raft_cmdpb.CmdType_Delete:
		wb.DeleteCF(req.Delete.Cf, req.Delete.Key)
	}

	// leader and previous leader can handle proposals
	d.handleProposal(entry, func(p *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			// apply then read
			d.peerStorage.applyState.AppliedIndex = entry.Index
			err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if err != nil {
				panic(err)
			}
			// apply to state machine
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)
			value, err := engine_util.GetCF(d.ctx.engine.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				value = nil
			}
			//log.Infof("%d apply get key %v value %v, apply index %v\n", d.RaftGroup.Raft.Id, key, value, entry.Index)
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{
						Value: value,
					},
				},
			}
			// new wb, prevent duplicate writes
			wb = &engine_util.WriteBatch{}
		case raft_cmdpb.CmdType_Put:
			//log.Infof("%d apply put key %s\n", d.RaftGroup.Raft.Id, key)
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				},
			}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				},
			}
		case raft_cmdpb.CmdType_Snap:
			var err *util.ErrEpochNotMatch
			if goerrors.As(util.CheckRegionEpoch(msg, d.Region(), true), &err) {
				p.cb.Done(ErrResp(err))
				return
			}
			// must write data util this entry
			d.peerStorage.applyState.AppliedIndex = entry.Index
			wbErr := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			if wbErr != nil {
				panic(wbErr)
				return
			}
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)
			wb = &engine_util.WriteBatch{}
			resp.Responses = []*raft_cmdpb.Response{
				{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				},
			}
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		p.cb.Done(resp)
	})
	return wb
}
func getPeerIndex(region *metapb.Region, id uint64) int {
	for i, peer := range region.Peers {
		if peer.Id == id {
			return i
		}
	}
	return -1
}
func (d *peerMsgHandler) applyConfChange(entry eraftpb.Entry, cc *eraftpb.ConfChange, wb *engine_util.WriteBatch) *engine_util.WriteBatch {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	region := d.Region()
	if goerrors.As(util.CheckRegionEpoch(msg, region, true), &err) {
		// FIXME epoch changed
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(ErrResp(err))
		})
		return wb
	}
	peerIndex := getPeerIndex(region, cc.NodeId)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if peerIndex == -1 {
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()

			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer += 1
			//save new region state
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			d.insertPeerCache(peer)

			storeMeta.regions[region.Id] = region
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
			storeMeta.Unlock()
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if d.Meta.Id == cc.NodeId {
			// do some hack
			d.startToDestroyPeer()
			return wb
		}
		if peerIndex != -1 {
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()

			region.Peers = append(region.Peers[:peerIndex], region.Peers[peerIndex+1:]...)
			region.RegionEpoch.ConfVer += 1
			meta.WriteRegionState(wb, region, rspb.PeerState_Normal)
			d.removePeerCache(cc.NodeId)
			storeMeta.regions[region.Id] = region

			storeMeta.Unlock()
			log.Infof("%s finish remove peer %d, now Prs %+v", d.Tag, cc.NodeId, region.Peers)
		}
	}
	d.RaftGroup.ApplyConfChange(*cc)
	// FIXME
	// notify scheduler
	d.handleProposal(entry, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: d.Region()},
			},
		})
	})
	return wb
}
func (d *peerMsgHandler) startToDestroyPeer() {
	if len(d.Region().Peers) == 2 && d.IsLeader() {
		var targetPeer uint64 = 0
		for _, peer := range d.Region().Peers {
			if peer.Id != d.PeerId() {
				targetPeer = peer.Id
				break
			}
		}
		if targetPeer == 0 {
			panic("This should not happen")
		}
		m := []eraftpb.Message{{
			To:      targetPeer,
			MsgType: eraftpb.MessageType_MsgAppend,
			Commit:  d.peerStorage.raftState.HardState.Commit,
			LogTerm: d.peerStorage.raftState.HardState.Term,
		}}
		for i := 0; i < 10; i++ {
			d.Send(d.ctx.trans, m)
			time.Sleep(100 * time.Millisecond)
		}
	}
	d.destroyPeer()
}
func (d *peerMsgHandler) handleProposal(entry eraftpb.Entry, handler func(p *proposal)) {
	if len(d.proposals) == 0 {
		return
	}
	p := d.proposals[0]
	if p.index == entry.Index {
		if p.term != entry.Term {
			// this proposal is now one a follower
			// the log entry has been overwritten by new leader
			NotifyStaleReq(entry.Term, p.cb)
		} else {
			handler(p)
		}
	}
	d.proposals = d.proposals[1:]
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
