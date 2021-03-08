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
package org.apache.iotdb.cluster.utils;

import java.io.File;
import java.io.IOException;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class RemoteFileLoad {

  private static final ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private static final String LOAD_FILES_FOLDER =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "loads";

  public static void loadRemoteFile(String path) throws IOException {
    SSHClient ssh = new SSHClient();
    ssh.addHostKeyVerifier(new PromiscuousVerifier());
    ssh.connect(config.getProxyClientIp());
    ssh.authPassword(config.getProxyUserName(), config.getProxyPassword());
    try {
      ssh.newSCPFileTransfer()
          .download(path, new FileSystemFile(LOAD_FILES_FOLDER + File.separator + path));
    } finally {
      ssh.disconnect();
    }
  }
}
