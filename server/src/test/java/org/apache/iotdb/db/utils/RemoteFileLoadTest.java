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

import org.junit.Test;

public class RemoteFileLoadTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Test
  public void loadRemoteFile2Test() {
    String ip = "172.16.48.4";
    String user = "root";
    String password = "2021fight";
    String remotePath = "/data8/qhl/data-migration/res.out";
    String fileName = "res.out";
    String folder = "root.group_9/0/0/";

    config.setProxyClientIp(ip);
    config.setProxyUserName(user);
    config.setProxyPassword(password);
    String result = RemoteFileLoad.loadRemoteFile(ip, remotePath, fileName, folder);
    System.out.println(result);
  }
}
