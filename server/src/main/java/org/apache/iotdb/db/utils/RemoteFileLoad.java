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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FileSystemFile;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RemoteFileLoad {

  private static final Logger logger = LoggerFactory.getLogger(RemoteFileLoad.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String LOAD_FILES_FOLDER = config.getSystemDir() + File.separator + "loads";
  private static final int loadStrategy = config.getLoadStrategy();

  public static String loadRemoteFileV1(String remotePath, String fileName, String folder) {
    return loadRemoteFile(config.getProxyClientIp(), remotePath, fileName, folder);
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

  public static String loadRemoteFile(
      String ip, String remotePath, String fileName, String folder) {
    if (loadStrategy == 1) {
      return loadRemoteFileV1(ip, remotePath, fileName, folder);
    } else {
      return loadRemoteFileV2(ip, remotePath, fileName, folder);
    }
  }

  public static String loadRemoteFileV1(
      String ip, String remotePath, String fileName, String folder) {
    SSHClient ssh = new SSHClient();
    String result = null;
    try {
      ssh.addHostKeyVerifier(new PromiscuousVerifier());
      ssh.connect(ip);
      ssh.authPassword(config.getProxyUserName(), config.getProxyPassword());
      String target = LOAD_FILES_FOLDER + File.separator + folder + File.separator + fileName;
      File newFile = new File(target);
      if (newFile.exists()) {
        return newFile.getAbsolutePath();
      }
      createIfFileAbsent(newFile);
      ssh.newSCPFileTransfer().download(remotePath, new FileSystemFile(target));
      result = newFile.getAbsolutePath();
    } catch (Exception e) {
      logger.error("v1, load file={} failed from ip={}", remotePath, ip, e);
    } finally {
      try {
        ssh.disconnect();
      } catch (IOException e) {
        logger.error("v1, close ssh connection failed, file={}, ip={}", remotePath, ip, e);
      }
    }
    logger.debug("v1, load remote file={} from ip={} to {}", remotePath, ip, result);
    return result;
  }

  public static String loadRemoteFileV2(
      String ip, String remotePath, String fileName, String folder) {
    String target = LOAD_FILES_FOLDER + File.separator + folder + File.separator + fileName;
    File newFile = new File(target);
    if (newFile.exists()) {
      return newFile.getAbsolutePath();
    }
    createIfFileAbsent(newFile);

    String result = null;
    try {
      JSch jsch = new JSch();
      Session session = jsch.getSession(config.getProxyUserName(), ip, 22);
      session.setPassword(config.getProxyPassword());
      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();
      FileOutputStream fos = null;
      try {
        // exec 'scp -f rfile' remotely
        remotePath = remotePath.replace("'", "'\"'\"'");
        remotePath = "'" + remotePath + "'";
        String command = "scp -f " + remotePath;
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        // get I/O streams for remote scp
        OutputStream out = channel.getOutputStream();
        InputStream in = channel.getInputStream();
        channel.connect();

        byte[] buf = new byte[32 * 1024];

        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();

        while (true) {
          int c = checkAck(in);
          if (c != 'C') {
            break;
          }
          // read '0644 '
          in.read(buf, 0, 5);
          long fileSize = 0L;
          while (true) {
            if (in.read(buf, 0, 1) < 0) {
              // error
              break;
            }
            if (buf[0] == ' ') {
              break;
            }
            fileSize = fileSize * 10L + (long) (buf[0] - '0');
          }

          String file = null;
          for (int i = 0; ; i++) {
            in.read(buf, i, 1);
            if (buf[i] == (byte) 0x0a) {
              file = new String(buf, 0, i);
              break;
            }
          }

          logger.debug("v2, fileSize={}, file={}", fileSize, file);

          // send '\0'
          buf[0] = 0;
          out.write(buf, 0, 1);
          out.flush();

          // read a content of lfile
          fos = new FileOutputStream(target);
          int foo;
          while (true) {
            if (buf.length < fileSize) {
              foo = buf.length;
            } else {
              foo = (int) fileSize;
            }
            foo = in.read(buf, 0, foo);
            if (foo < 0) {
              // error
              break;
            }
            fos.write(buf, 0, foo);
            fileSize -= foo;
            if (fileSize == 0L) {
              break;
            }
          }
          fos.close();
          fos = null;

          // send '\0'
          buf[0] = 0;
          out.write(buf, 0, 1);
          out.flush();
        }
        session.disconnect();
        result = newFile.getAbsolutePath();
      } catch (Exception e) {
        logger.error("v2, error occurred when load, file={}", remotePath, e);
        try {
          if (fos != null) {
            fos.close();
          }
        } catch (Exception ee) {
          logger.error("v2, close occurred when load, file={}", remotePath, ee);
        }
      }
    } catch (Exception e) {
      logger.error("v2, error occurred when load one tsfile, file={}", remotePath, e);
    }
    logger.debug("v2, load remote file={} from ip={} to {}", remotePath, ip, result);
    return result;
  }

  static int checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //          -1
    if (b == 0) {
      return b;
    }
    if (b == -1) {
      return b;
    }

    if (b == 1 || b == 2) {
      StringBuffer sb = new StringBuffer();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      } while (c != '\n');
      if (b == 1) { // error
        logger.error("check first failed=" + sb.toString());
      }
      if (b == 2) { // fatal error
        logger.error("check second failed=" + sb.toString());
      }
    }
    return b;
  }
}
