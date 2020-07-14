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

package org.apache.iotdb.cluster.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncProcessor;
import org.apache.iotdb.cluster.utils.CallQueue;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RaftServer works as a broker (network and protocol layer) that sends the requests to the proper
 * RaftMembers to process.
 */
public abstract class RaftServer implements RaftService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);
  private static int connectionTimeoutInMS =
      ClusterDescriptor.getInstance().getConfig().getConnectionTimeoutInMS();
  private static int queryTimeoutInSec =
      ClusterDescriptor.getInstance().getConfig().getQueryTimeoutInSec();
  private static int syncLeaderMaxWaitMs = 20 * 1000;
  private static long heartBeatIntervalMs = 1000L;

  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  // the socket poolServer will listen to
  private TNonblockingServerTransport socket;
  private TNonblockingServerTransport heartbeatSocket;
  // RPC processing server
  private TServer poolServer;

  private TServer heartbeatPoolServer;
  Node thisNode;

  TProtocolFactory protocolFactory = config.isRpcThriftCompressionEnabled() ?
      new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

  TProtocolFactory heartbeatProtocolFactory = config.isRpcThriftCompressionEnabled() ?
      new TCompactProtocol.Factory() : new TBinaryProtocol.Factory();

  // this thread pool is to run the thrift server (poolServer above)
  private ExecutorService clientService;

  // this thread pool is to run the thrift server (poolServer above)
  private ExecutorService heartbeatClientService;

  RaftServer() {
    thisNode = new Node();
    thisNode.setIp(config.getLocalIP());
    thisNode.setMetaPort(config.getLocalMetaPort());
    thisNode.setDataPort(config.getLocalDataPort());
    thisNode.setHeartbeatDataPort(config.getLocalDataPort() + 1);
    thisNode.setHeartbeatMetaPort(config.getLocalMetaPort() + 1);

  }

  RaftServer(Node thisNode) {
    this.thisNode = thisNode;
  }

  public static int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public static void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    RaftServer.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public static int getQueryTimeoutInSec() {
    return queryTimeoutInSec;
  }

  public static int getSyncLeaderMaxWaitMs() {
    return syncLeaderMaxWaitMs;
  }

  public static void setSyncLeaderMaxWaitMs(int syncLeaderMaxWaitMs) {
    RaftServer.syncLeaderMaxWaitMs = syncLeaderMaxWaitMs;
  }

  public static long getHeartBeatIntervalMs() {
    return heartBeatIntervalMs;
  }

  public static void setHeartBeatIntervalMs(long heartBeatIntervalMs) {
    RaftServer.heartBeatIntervalMs = heartBeatIntervalMs;
  }

  /**
   * Establish a thrift server with the configurations in ClusterConfig to listen to and respond to
   * thrift RPCs. Calling the method twice does not induce side effects.
   *
   * @throws TTransportException
   */
  @SuppressWarnings("java:S1130") // thrown in override method
  public void start() throws TTransportException, StartupException {
    if (poolServer != null) {
      return;
    }

    establishServer();

    establishHeartbeatServer();
  }

  /**
   * Stop the thrift server, close the socket and interrupt all in progress RPCs. Calling the method
   * twice does not induce side effects.
   */
  public void stop() {
    if (poolServer == null) {
      return;
    }

    poolServer.stop();
    socket.close();
    clientService.shutdownNow();
    socket = null;
    poolServer = null;
  }

  /**
   * @return An AsyncProcessor that contains the extended interfaces of a non-abstract subclass of
   * RaftService (DataService or MetaService).
   */
  abstract AsyncProcessor getProcessor();

  /**
   * @return A socket that will be used to establish a thrift server to listen to RPC requests.
   * DataServer and MetaServer use different port, so this is to be determined.
   * @throws TTransportException
   */
  abstract TNonblockingServerSocket getServerSocket() throws TTransportException;

  abstract TNonblockingServerSocket getHeartbeatServerSocket() throws TTransportException;

  /**
   * Each thrift RPC request will be processed in a separate thread and this will return the name
   * prefix of such threads. This is used to fast distinguish DataServer and MetaServer in the logs
   * for the sake of debug.
   *
   * @return name prefix of RPC processing threads.
   */
  abstract String getClientThreadPrefix();

  /**
   * The thrift server will be run in a separate thread, and this will be its name. It help you
   * locate the desired logs quickly when debugging.
   *
   * @return The name of the thread running the thrift server.
   */
  abstract String getServerClientName();

  private void establishServer() throws TTransportException {
    logger.info("Cluster node {} begins to set up", thisNode);

    socket = getServerSocket();
    Args poolArgs =
        new THsHaServer.Args(socket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(Runtime.getRuntime().availableProcessors());

//    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
//        poolArgs.maxWorkerThreads, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
//        new SynchronousQueue<>(), new ThreadFactory() {
//      private AtomicLong threadIndex = new AtomicLong(0);
//
//      @Override
//      public Thread newThread(Runnable r) {
//        return new Thread(r, getClientThreadPrefix() + threadIndex.incrementAndGet());
//      }
//    }));

    CallQueue callQueue = new CallQueue(new LinkedBlockingQueue<>(1000));
    ExecutorService executorService = createExecutor(
        callQueue, poolArgs.minWorkerThreads, poolArgs.maxWorkerThreads);
    poolArgs.executorService(executorService);
    poolArgs.processor(getProcessor());
    poolArgs.protocolFactory(protocolFactory);
    // async service requires FramedTransport
    poolArgs.transportFactory(new TFastFramedTransport.Factory(
        IoTDBDescriptor.getInstance().getConfig().getThriftInitBufferSize(),
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize()));

    // run the thrift server in a separate thread so that the main thread is not blocked
    poolServer = new THsHaServer(poolArgs);
    clientService = Executors.newSingleThreadExecutor(r -> new Thread(r, getServerClientName()));
    clientService.submit(() -> poolServer.serve());

    logger.info("Cluster node {} is up", thisNode);
  }

  private ExecutorService createExecutor(BlockingQueue<Runnable> callQueue,
      int minWorkers, int maxWorkers) {
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift-worker-%d");
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(minWorkers, maxWorkers,
        Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build());
    threadPool.allowCoreThreadTimeOut(true);
    return threadPool;
  }

  private void establishHeartbeatServer() throws TTransportException {
    logger.info("heartbeat cluster node {} begins to set up", thisNode);

    heartbeatSocket = getHeartbeatServerSocket();
    Args poolArgs =
        new THsHaServer.Args(heartbeatSocket).maxWorkerThreads(config.getMaxConcurrentClientNum())
            .minWorkerThreads(Runtime.getRuntime().availableProcessors());

//    poolArgs.executorService(new ThreadPoolExecutor(poolArgs.minWorkerThreads,
//        poolArgs.maxWorkerThreads, poolArgs.getStopTimeoutVal(), poolArgs.getStopTimeoutUnit(),
//        new SynchronousQueue<>(), new ThreadFactory() {
//      private AtomicLong threadIndex = new AtomicLong(0);
//
//      @Override
//      public Thread newThread(Runnable r) {
//        return new Thread(r, getClientThreadPrefix() + threadIndex.incrementAndGet());
//      }
//    }));

    CallQueue callQueue = new CallQueue(new LinkedBlockingQueue<>(1000));
    ExecutorService executorService = createExecutor(
        callQueue, poolArgs.minWorkerThreads, poolArgs.maxWorkerThreads);
    poolArgs.executorService(executorService);

    poolArgs.processor(getProcessor());
    poolArgs.protocolFactory(heartbeatProtocolFactory);
    // async service requires FramedTransport
    poolArgs.transportFactory(new TFastFramedTransport.Factory(
        IoTDBDescriptor.getInstance().getConfig().getThriftInitBufferSize(),
        IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize()));

    // run the thrift server in a separate thread so that the main thread is not blocked
    heartbeatPoolServer = new THsHaServer(poolArgs);

    heartbeatClientService = Executors.newSingleThreadExecutor(r -> new Thread(r, getServerClientName()));
    heartbeatClientService.submit(() -> heartbeatPoolServer.serve());

    logger.info("establishHeartbeatServer Cluster node {} is up", thisNode);
  }
}
