/*
 * Copyright 2017-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zero

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/x"
	"github.com/golang/glog"
)

var (
	emptyNum         pb.Num
	emptyAssignedIds pb.AssignedIds
)

const (
	leaseBandwidth = uint64(10000)
)

func (s *Server) updateLeases() {
	var startTs uint64
	s.Lock()
	s.nextLeaseId = s.state.MaxLeaseId + 1
	s.nextTxnTs = s.state.MaxTxnTs + 1
	startTs = s.nextTxnTs
	glog.Infof("Updated Lease id: %d. Txn Ts: %d", s.nextLeaseId, s.nextTxnTs)
	s.Unlock()
	s.orc.updateStartTxnTs(startTs)
}

func (s *Server) maxLeaseId() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.state.MaxLeaseId
}

func (s *Server) maxTxnTs() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.state.MaxTxnTs
}

var errServedFromMemory = errors.New("lease was served from memory")

// lease would either allocate ids or timestamps.
// This function is triggered by an RPC call. We ensure that only leader can assign new UIDs,
// so we can tackle any collisions that might happen with the leasemanager
// In essence, we just want one server to be handing out new uids.
func (s *Server) lease(ctx context.Context, num *pb.Num, txn bool) (*pb.AssignedIds, error) {
	node := s.Node
	// TODO: Fix when we move to linearizable reads, need to check if we are the leader, might be
	// based on leader leases. If this node gets partitioned and unless checkquorum is enabled, this
	// node would still think that it's the leader.
	if !node.AmLeader() {
		return &emptyAssignedIds, x.Errorf("Assigning IDs is only allowed on leader.")
	}

	if num.Val == 0 && !num.ReadOnly {
		return &emptyAssignedIds, x.Errorf("Nothing to be leased")
	}
	if glog.V(3) {
		glog.Infof("Got lease request for txn: %v. Num: %+v\n", txn, num)
	}

	s.leaseLock.Lock()
	defer s.leaseLock.Unlock()

	if txn {
		if num.Val == 0 && num.ReadOnly {
			// If we're only asking for a readonly timestamp, we can potentially
			// service it directly.
			if glog.V(3) {
				glog.Infof("Attempting to serve read only txn ts [%d, %d]",
					s.readOnlyTs, s.nextTxnTs)
			}
			if s.readOnlyTs > 0 && s.readOnlyTs == s.nextTxnTs-1 {
				return &pb.AssignedIds{ReadOnly: s.readOnlyTs}, errServedFromMemory
			}
		}
		// We couldn't service it. So, let's request an extra timestamp for
		// readonly transactions, if needed.
	}

	// If we're asking for more ids than the standard lease bandwidth, then we
	// should set howMany generously, so we can service future requests from
	// memory, without asking for another lease. Only used if we need to renew
	// our lease.
	howMany := leaseBandwidth
	if num.Val > leaseBandwidth {
		howMany = num.Val + leaseBandwidth
	}

	if s.nextLeaseId == 0 || s.nextTxnTs == 0 {
		return nil, errors.New("Server not initialized.")
	}

	var maxLease, available uint64
	var proposal pb.ZeroProposal

	// Calculate how many ids do we have available in memory, before we need to
	// renew our lease.
	if txn {
		maxLease = s.maxTxnTs()
		available = maxLease - s.nextTxnTs + 1
		proposal.MaxTxnTs = maxLease + howMany
	} else {
		maxLease = s.maxLeaseId()
		available = maxLease - s.nextLeaseId + 1
		proposal.MaxLeaseId = maxLease + howMany
	}

	// If we have less available than what we need, we need to renew our lease.
	if available < num.Val+1 { // +1 for a potential readonly ts.
		// Blocking propose to get more ids or timestamps.
		if err := s.Node.proposeAndWait(ctx, &proposal); err != nil {
			return nil, err
		}
	}

	out := &pb.AssignedIds{}
	if txn {
		if num.Val > 0 {
			out.StartId = s.nextTxnTs
			out.EndId = out.StartId + num.Val - 1
			s.nextTxnTs = out.EndId + 1
		}
		if num.ReadOnly {
			s.readOnlyTs = s.nextTxnTs
			s.nextTxnTs++
			out.ReadOnly = s.readOnlyTs
		}
		s.orc.doneUntil.Begin(x.Max(out.EndId, out.ReadOnly))
	} else {
		out.StartId = s.nextLeaseId
		out.EndId = out.StartId + num.Val - 1
		s.nextLeaseId = out.EndId + 1
	}
	return out, nil
}

// AssignUids is used to assign new uids by communicating with the leader of the RAFT group
// responsible for handing out uids.
func (s *Server) AssignUids(ctx context.Context, num *pb.Num) (*pb.AssignedIds, error) {
	if ctx.Err() != nil {
		return &emptyAssignedIds, ctx.Err()
	}

	reply := &emptyAssignedIds
	c := make(chan error, 1)
	go func() {
		var err error
		if s.Node.AmLeader() {
			reply, err = s.lease(ctx, num, false)
			c <- err
			return
		}
		pl := s.Leader(0)
		if pl == nil {
			err = x.Errorf("No healthy connection found to Leader of group zero")
		} else {
			zc := pb.NewZeroClient(pl.Get())
			reply, err = zc.AssignUids(ctx, num)
		}
		c <- err
	}()

	select {
	case <-ctx.Done():
		return reply, ctx.Err()
	case err := <-c:
		return reply, err
	}
}
