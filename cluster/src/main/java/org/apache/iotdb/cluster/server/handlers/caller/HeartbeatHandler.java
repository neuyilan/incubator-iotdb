/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.server.handlers.caller;

import static org.apache.iotdb.cluster.server.Response.RESPONSE_AGREE;

import java.net.ConnectException;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HeartbeatHandler checks the response of a heartbeat and decides whether to start a catch-up or
 * give up the leadership due to the term is stale.
 */
public class HeartbeatHandler implements AsyncMethodCallback<HeartBeatResponse> {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatHandler.class);

  private RaftMember localMember;
  private String memberName;
  private Node receiver;

  public HeartbeatHandler(RaftMember localMember, Node receiver) {
    this.localMember = localMember;
    this.receiver = receiver;
    this.memberName = localMember.getName();
  }

  @Override
  public void onComplete(HeartBeatResponse resp) {
    logger.trace("{}: Received a heartbeat response", memberName);
    long followerTerm = resp.getTerm();
    if (followerTerm == RESPONSE_AGREE) {
      // current leadership is still valid
      localMember.processValidHeartbeatResp(resp, receiver);

      Node follower = resp.getFollower();
      long lastLogIdx = resp.getLastLogIndex();
      long lastLogTerm = resp.getLastLogTerm();
      long localLastLogIdx = localMember.getLogManager().getLastLogIndex();
      long localLastLogTerm = localMember.getLogManager().getLastLogTerm();
      logger.trace("{}: Node {} is still alive, log index: {}/{}, log term: {}/{}",
          memberName, follower, lastLogIdx
          , localLastLogIdx, lastLogTerm, localLastLogTerm);

      Peer peer = localMember.getPeerMap()
          .computeIfAbsent(follower, k -> new Peer(localMember.getLogManager().getLastLogIndex()));
      if (!peer.isCatchUp() || !localMember.getLogManager()
          .isLogUpToDate(lastLogTerm, lastLogIdx)) {
        peer.setNextIndex(lastLogIdx + 1);
        logger.debug("{}: catching up node {}, index-term: {}-{}/{}-{}, peer nextIndex {}, peer "
                + "match index {}",
            memberName, follower,
            lastLogIdx, lastLogTerm,
            localLastLogIdx, localLastLogTerm,
            peer.getNextIndex(), peer.getMatchIndex());
        localMember.catchUp(follower);
      } else {
        peer.setMatchIndex(Math.max(peer.getMatchIndex(), lastLogIdx));
      }
    } else {
      // current leadership is invalid because the follower has a larger term
      synchronized (localMember.getTerm()) {
        logger.info("add by qihouliang, heartbearHandler onComplete, name={}, owner={}", localMember.getName(),
            localMember.getReentrantLockClass().owner());
        localMember.getReentrantLockClass().lock();
        long currTerm = localMember.getTerm().get();
        if (currTerm < followerTerm) {
          logger.info("{}: Losing leadership because current term {} is smaller than {}",
              memberName, currTerm, followerTerm);
          localMember.stepDown(followerTerm, false);
        }
        localMember.getReentrantLockClass().unlock();
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    if (exception instanceof ConnectException) {
      logger.warn("{}: Cannot connect to {}: {}", memberName, receiver, exception.getMessage());
    } else {
      logger.error("{}: Heart beat error, receiver {}, {}", memberName, receiver,
          exception.getMessage());
    }
  }
}
