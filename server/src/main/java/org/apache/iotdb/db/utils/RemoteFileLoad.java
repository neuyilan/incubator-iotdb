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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class RemoteFileLoad {

  private static final Logger logger = LoggerFactory.getLogger(RemoteFileLoad.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String LOAD_FILES_FOLDER =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "loads";

  public static String loadRemoteFile(String remotePath, String fileName) {
    return loadRemoteFile(config.getProxyClientIp(), remotePath, fileName);
  }

  private static void createIfFileAbsent(File file) {
    try {
      if (!file.exists()) {
        if (!file.getParentFile().exists()) {
          FileUtils.forceMkdir(file.getParentFile());
        }
      }
    } catch (Exception e) {
      logger.error("error occurred while processing path={}", file.getAbsolutePath(), e);
    }
  }

  public static String loadRemoteFile(String ip, String remotePath, String fileName) {
    SSHClient ssh = new SSHClient();
    String result = null;
    try {
      ssh.addHostKeyVerifier(new PromiscuousVerifier());
      ssh.connect(ip);
      ssh.authPassword(config.getProxyUserName(), config.getProxyPassword());
      String target = LOAD_FILES_FOLDER + File.separator + fileName;
      File newFile = new File(target);
      if (newFile.exists()) {
        return newFile.getAbsolutePath();
      }
      createIfFileAbsent(newFile);
      ssh.newSCPFileTransfer().download(remotePath, new FileSystemFile(target));
      result = newFile.getAbsolutePath();
    } catch (Exception e) {
      logger.error("load file={} failed from ip={}", remotePath, ip, e);
    } finally {
      try {
        ssh.disconnect();
      } catch (IOException e) {
        logger.error("close ssh connection failed, file={}, ip={}", remotePath, ip, e);
      }
    }
    logger.debug("load remote file={} from ip={} to {}", remotePath, ip, result);
    return result;
  }
}
