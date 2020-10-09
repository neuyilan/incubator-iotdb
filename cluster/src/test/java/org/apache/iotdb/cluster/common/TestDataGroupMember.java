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

package org.apache.iotdb.cluster.common;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.manage.FilePartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotManager;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;

public class TestDataGroupMember extends DataGroupMember {

  public TestDataGroupMember() {
    super();
    setQueryManager(new ClusterQueryManager());
    this.slotManager = new SlotManager(ClusterConstant.SLOT_NUM, null);
    this.allNodes = Collections.singletonList(TestUtils.getNode(0));
    PartitionTable partitionTable = TestUtils.getPartitionTable(3);
    FilePartitionedSnapshotLogManager manager = new FilePartitionedSnapshotLogManager(
        new TestLogApplier(), partitionTable, TestUtils.getNode(0), TestUtils.getNode(0), this);
    setLogManager(manager);
  }

  public TestDataGroupMember(Node thisNode, List<Node> allNodes) {
    super();
    this.thisNode = thisNode;
    this.allNodes = allNodes;
    this.slotManager = new SlotManager(ClusterConstant.SLOT_NUM, null);
    setQueryManager(new ClusterQueryManager());
    PartitionTable partitionTable = TestUtils.getPartitionTable(0);
    FilePartitionedSnapshotLogManager manager = new FilePartitionedSnapshotLogManager(
        new TestLogApplier(), partitionTable, TestUtils.getNode(0), thisNode, this);
    setLogManager(manager);
  }

  private Map<Long, Log> appliedLogs;
  private volatile boolean blocked = false;
  private LogApplier logApplier = new TestLogApplier() {
    @Override
    public void apply(Log log) {
      while (blocked) {
        // stuck
      }
      appliedLogs.put(log.getCurrLogIndex(), log);
      log.setApplied(true);
    }
  };
}
