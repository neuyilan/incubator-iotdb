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

package org.apache.iotdb.cluster.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LogDispatcher servers a raft leader by queuing logs that the leader wants to send to the
 * follower and send the logs in an ordered manner so that the followers will not wait for previous
 * logs for too long. For example: if the leader send 3 logs, log1, log2, log3, concurrently to
 * follower A, the actual reach order may be log3, log2, and log1. According to the protocol, log3
 * and log2 must halt until log1 reaches, as a result, the total delay may increase significantly.
 */
public class LogDispatcher {

  private static final Logger logger = LoggerFactory.getLogger(LogDispatcher.class);
  private RaftMember member;
  private List<BlockingQueue<SendLogRequest>> nodeLogQueues =
      new ArrayList<>();
  private Map<Integer, Node> nodeIntegerMap;
  private ExecutorService executorService;

  public LogDispatcher(RaftMember member) {
    nodeIntegerMap = new ConcurrentHashMap();
    this.member = member;
    executorService = Executors.newCachedThreadPool();
    int i = 0;
    for (Node node : member.getAllNodes()) {
      if (!node.equals(member.getThisNode())) {
        nodeLogQueues.add(createQueueAndBindingThread(node));
        nodeIntegerMap.put(i, node);
        i++;
        logger.debug("qhl, {}, node={}", member.getName(), node.toString());
      }
    }

  }

  public boolean offer(SendLogRequest log) {
    for (int i = 0; i < nodeLogQueues.size(); i++) {
      BlockingQueue<SendLogRequest> nodeLogQueue = nodeLogQueues.get(i);
      logger.debug("qhl,{}, nodeLogQueue size={}, i={}, receiver={}", member.getName(),
          nodeLogQueue.size(), i, nodeIntegerMap.get(i));
      try {
        boolean success = nodeLogQueue.offer(log, 60, TimeUnit.SECONDS);
        if (!success) {
          return false;
        }
      } catch (InterruptedException e) {
        logger.error("Interrupted when inserting {} into queue[{}]", log, i);
        Thread.currentThread().interrupt();
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} is enqueued in {} queues", log.log, nodeLogQueues.size());
    }
    return true;
  }

  private BlockingQueue<SendLogRequest> createQueueAndBindingThread(Node node) {
    BlockingQueue<SendLogRequest> logBlockingQueue =
        new ArrayBlockingQueue<>(4096);
    int bindingThreadNum = 1;
    for (int i = 0; i < bindingThreadNum; i++) {
      executorService.submit(new DispatcherThread(node, logBlockingQueue));
    }
    return logBlockingQueue;
  }

  public static class SendLogRequest {

    private Log log;
    private AtomicInteger voteCounter;
    private AtomicBoolean leaderShipStale;
    private AtomicLong newLeaderTerm;
    private AppendEntryRequest appendEntryRequest;

    public SendLogRequest(Log log, AtomicInteger voteCounter,
        AtomicBoolean leaderShipStale, AtomicLong newLeaderTerm,
        AppendEntryRequest appendEntryRequest) {
      this.log = log;
      this.setVoteCounter(voteCounter);
      this.setLeaderShipStale(leaderShipStale);
      this.setNewLeaderTerm(newLeaderTerm);
      this.setAppendEntryRequest(appendEntryRequest);
    }

    public AtomicInteger getVoteCounter() {
      return voteCounter;
    }

    public void setVoteCounter(AtomicInteger voteCounter) {
      this.voteCounter = voteCounter;
    }

    public AtomicBoolean getLeaderShipStale() {
      return leaderShipStale;
    }

    public void setLeaderShipStale(AtomicBoolean leaderShipStale) {
      this.leaderShipStale = leaderShipStale;
    }

    public AtomicLong getNewLeaderTerm() {
      return newLeaderTerm;
    }

    void setNewLeaderTerm(AtomicLong newLeaderTerm) {
      this.newLeaderTerm = newLeaderTerm;
    }

    public AppendEntryRequest getAppendEntryRequest() {
      return appendEntryRequest;
    }

    public void setAppendEntryRequest(
        AppendEntryRequest appendEntryRequest) {
      this.appendEntryRequest = appendEntryRequest;
    }
  }

  class DispatcherThread implements Runnable {

    private Node receiver;
    private BlockingQueue<SendLogRequest> logBlockingDeque;
    private List<SendLogRequest> currBatch = new ArrayList<>();

    DispatcherThread(Node receiver,
        BlockingQueue<SendLogRequest> logBlockingDeque) {
      this.receiver = receiver;
      this.logBlockingDeque = logBlockingDeque;
      logger.debug("qhl {}, receiver={}", member.getName(), receiver.toString());
    }

    @Override
    public void run() {
      Thread.currentThread().setName("LogDispatcher-" + member.getName() + "-" + receiver);
      logger
          .debug("qhl, start {}, thread name={}", member.getName(),
              Thread.currentThread().getName());
      try {
        while (!Thread.interrupted()) {
          SendLogRequest poll = logBlockingDeque.peek();
          if (poll == null) {
            continue;
          }

//          currBatch.add(poll);
//          logBlockingDeque.drainTo(currBatch);
          if (logger.isDebugEnabled()) {
            logger.debug("Sending {} logs to {}", currBatch.size(), receiver);
          }
          boolean success = sendLog(poll);
          if (success) {
            logBlockingDeque.remove();
          }
//          for (SendLogRequest request : currBatch) {
//            sendLog(request);
//          }
//          currBatch.clear();
          if (logger.isDebugEnabled()) {
            logger.debug("end of Sending {} logs to {} ", currBatch.size(), receiver);
          }
        }
      } catch (Exception e) {
        logger.error("Unexpected error in log dispatcher", e);
      }
      logger.info("Dispatcher exits");
    }

    private boolean sendLog(SendLogRequest logRequest) {
      boolean success = member
          .sendLogToFollower(logRequest.log, logRequest.getVoteCounter(), receiver,
              logRequest.getLeaderShipStale(), logRequest.getNewLeaderTerm(),
              logRequest.getAppendEntryRequest());
      return success;
    }
  }
}
