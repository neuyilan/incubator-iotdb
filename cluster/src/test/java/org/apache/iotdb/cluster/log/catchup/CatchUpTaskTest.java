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

package org.apache.iotdb.cluster.log.catchup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.common.TestAsyncClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestSyncClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CatchUpTaskTest {

  private List<Log> receivedLogs = new ArrayList<>();
  private long leaderCommit;
  private Node header = new Node();
  private boolean prevUseAsyncServer;


  private RaftMember sender = new TestDataGroupMember() {
//    @Override
//    public PartitionTable getPartitionTable() {
//      return new SlotPartitionTable(TestUtils.getNode(0));
//    }


    @Override
    public Client getSyncClient(Node node) {
      return new TestSyncClient() {
        @Override
        public long appendEntry(AppendEntryRequest request) {
          return dummyAppendEntry(request);
        }

        @Override
        public long appendEntries(AppendEntriesRequest request) {
          return dummyAppendEntries(request);
        }

        @Override
        public boolean matchTerm(long index, long term, Node header) {
          return dummyMatchTerm(index, term);
        }

        @Override
        public void sendSnapshot(SendSnapshotRequest request) {
          // do nothing
        }
      };
    }

    @Override
    public AsyncClient getAsyncClient(Node node) {
      return new TestAsyncClient() {
        @Override
        public void appendEntry(AppendEntryRequest request,
            AsyncMethodCallback<Long> resultHandler) {
          new Thread(() -> resultHandler.onComplete(dummyAppendEntry(request))).start();
        }

        @Override
        public void appendEntries(AppendEntriesRequest request,
            AsyncMethodCallback<Long> resultHandler) {
          new Thread(() -> resultHandler.onComplete(dummyAppendEntries(request))).start();
        }

        @Override
        public void matchTerm(long index, long term, Node header,
            AsyncMethodCallback<Boolean> resultHandler) {
          new Thread(() -> resultHandler.onComplete(dummyMatchTerm(index, term))).start();
        }

        @Override
        public void sendSnapshot(SendSnapshotRequest request,
            AsyncMethodCallback<Void> resultHandler) {
          System.out.println("111111111=" + request.getSnapshotBytes());
          PartitionedSnapshot<FileSnapshot> snapshot = new PartitionedSnapshot<>(FileSnapshot::new);
          snapshot.deserialize(ByteBuffer.wrap(request.getSnapshotBytes()));
          System.out.println("222222222222=" + snapshot.toString());
          new Thread(() -> resultHandler.onComplete(null)).start();
        }
      };
    }

    @Override
    public Node getHeader() {
      return header;
    }
  };

  private long dummyAppendEntry(AppendEntryRequest request) {
    Log log = receivedLogs.get(receivedLogs.size() - 1);
    Log testLog;
    try {
      testLog = LogParser.getINSTANCE().parse(request.entry);
    } catch (Exception e) {
      return Response.RESPONSE_NULL;
    }
    if (testLog.getCurrLogIndex() == log.getCurrLogIndex() + 1) {
      leaderCommit = Math.max(request.leaderCommit, leaderCommit);
      receivedLogs.add(testLog);
      return Response.RESPONSE_AGREE;
    }
    if (testLog.getCurrLogIndex() == log.getCurrLogIndex()) {
      leaderCommit = Math.max(request.leaderCommit, leaderCommit);
      return Response.RESPONSE_AGREE;
    }
    return Response.RESPONSE_LOG_MISMATCH;
  }

  private long dummyAppendEntries(AppendEntriesRequest request) {
    for (ByteBuffer byteBuffer : request.getEntries()) {
      Log testLog;
      try {
        testLog = LogParser.getINSTANCE().parse(byteBuffer);
      } catch (Exception e) {
        return Response.RESPONSE_NULL;
      }
      receivedLogs.add(testLog);
    }
    leaderCommit = Math.max(request.leaderCommit, leaderCommit);
    return Response.RESPONSE_AGREE;
  }

  private boolean dummyMatchTerm(long index, long term) {
    if (receivedLogs.isEmpty()) {
      return true;
    } else {
      for (Log receivedLog : receivedLogs) {
        if (receivedLog.getCurrLogTerm() == term
            && receivedLog.getCurrLogIndex() == index) {
          return true;
        }
      }
    }
    return false;
  }

  @Before
  public void setUp() {
    IoTDB.metaManager.init();
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    receivedLogs = new ArrayList<>();
    EmptyContentLog log = new EmptyContentLog();
    log.setCurrLogIndex(-1);
    log.setCurrLogTerm(-1);
    receivedLogs.add(log);
  }

  @After
  public void tearDown() throws Exception {
    IoTDB.metaManager.clear();
    sender.stop();
    EnvironmentUtils.cleanAllDir();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
  }

  @Test
  public void testCatchUpEmpty() throws LogExecutionException {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    receivedLogs.clear();

    sender.getLogManager().append(logList);
    sender.getLogManager().commitTo(9, false);
    sender.getLogManager().setMaxHaveAppliedCommitIndex(sender.getLogManager().getCommitLogIndex());
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    Peer peer = new Peer(10);
    peer.setMatchIndex(9);
    CatchUpTask task = new CatchUpTask(receiver, peer, sender);
    task.run();

    assertTrue(receivedLogs.isEmpty());
  }

  @Test
  public void testPartialCatchUpAsync() throws LogExecutionException {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
      if (i < 6) {
        receivedLogs.add(log);
      }
    }
    sender.getLogManager().append(logList);
    sender.getLogManager().commitTo(9, false);
    sender.getLogManager().setMaxHaveAppliedCommitIndex(sender.getLogManager().getCommitLogIndex());
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    Peer peer = new Peer(10);
    peer.setCatchUp(false);
    peer.setMatchIndex(0);
    CatchUpTask task = new CatchUpTask(receiver, peer, sender);
    task.run();

    assertEquals(logList, receivedLogs.subList(1, receivedLogs.size()));
    assertEquals(9, leaderCommit);
  }

  @Test
  public void testPartialCatchUpSync() throws LogExecutionException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);

    try {
      List<Log> logList = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        Log log = new EmptyContentLog();
        log.setCurrLogIndex(i);
        log.setCurrLogTerm(i);
        logList.add(log);
        if (i < 6) {
          receivedLogs.add(log);
        }
      }
      sender.getLogManager().append(logList);
      sender.getLogManager().commitTo(9, false);
      sender.getLogManager()
          .setMaxHaveAppliedCommitIndex(sender.getLogManager().getCommitLogIndex());
      Node receiver = new Node();
      sender.setCharacter(NodeCharacter.LEADER);
      Peer peer = new Peer(10);
      peer.setCatchUp(false);
      peer.setMatchIndex(0);
      CatchUpTask task = new CatchUpTask(receiver, peer, sender);
      task.run();

      assertEquals(logList, receivedLogs.subList(1, receivedLogs.size()));
      assertEquals(9, leaderCommit);
    } finally {
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testCatchUpSingle() throws Exception {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    sender.getLogManager().append(logList);
    sender.getLogManager().commitTo(9, false);
    // wait log is applied
    long startTime = System.currentTimeMillis();
    for (Log log : logList) {
      while (!log.isApplied()) {
        if ((System.currentTimeMillis() - startTime) > 60_000) {
          System.out.println("apply log time out");
          assertTrue(false);
        }
      }
      System.out.println("apply log success");
    }
    long max = sender.getLogManager().getMaxHaveAppliedCommitIndex();
    System.out.println("max=" + max);
//    sender.getLogManager().setMaxHaveAppliedCommitIndex(sender.getLogManager().getCommitLogIndex());
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    Peer peer = new Peer(10);
    peer.setCatchUp(false);
    peer.setNextIndex(0);
    CatchUpTask task = new CatchUpTask(receiver, peer, sender);
    ClusterDescriptor.getInstance().getConfig().setUseBatchInLogCatchUp(false);
    task.run();

    assertEquals(logList, receivedLogs.subList(1, receivedLogs.size()));
    assertEquals(9, leaderCommit);
  }

  @Test
  public void testCatchUpBatch() throws Exception {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    sender.getLogManager().append(logList);
    sender.getLogManager().commitTo(9, false);
    sender.getLogManager().setMaxHaveAppliedCommitIndex(sender.getLogManager().getCommitLogIndex());
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    Peer peer = new Peer(10);
    peer.setCatchUp(false);
    peer.setNextIndex(0);
    CatchUpTask task = new CatchUpTask(receiver, peer, sender);
    task.run();

    assertEquals(logList, receivedLogs.subList(1, receivedLogs.size()));
    assertEquals(9, leaderCommit);
  }
}