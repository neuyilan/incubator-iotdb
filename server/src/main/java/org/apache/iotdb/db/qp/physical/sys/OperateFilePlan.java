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
package org.apache.iotdb.db.qp.physical.sys;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class OperateFilePlan extends PhysicalPlan {

  private File file;
  private File targetDir;
  private boolean autoCreateSchema;
  private int sgLevel;
  private boolean isRemoteFile = false;
  private String remoteIp;

  public OperateFilePlan(OperatorType operatorType) {
    super(false, operatorType);
  }

  public OperateFilePlan(File file, OperatorType operatorType) {
    super(false, operatorType);
    this.file = file;
  }

  public OperateFilePlan(
      File file, OperatorType operatorType, boolean autoCreateSchema, int sgLevel) {
    super(false, operatorType);
    this.file = file;
    this.autoCreateSchema = autoCreateSchema;
    this.sgLevel = sgLevel;
  }

  public OperateFilePlan(
      File file, OperatorType operatorType, boolean autoCreateSchema, int sgLevel,
      boolean isRemoteFile, String remoteIp) {
    this(file, operatorType, autoCreateSchema, sgLevel);
    this.isRemoteFile = isRemoteFile;
    this.remoteIp = remoteIp;
  }

  public OperateFilePlan(File file, File targetDir, OperatorType operatorType) {
    super(false, operatorType);
    this.file = file;
    this.targetDir = targetDir;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public File getFile() {
    return file;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public boolean isAutoCreateSchema() {
    return autoCreateSchema;
  }

  public int getSgLevel() {
    return sgLevel;
  }

  public boolean isRemoteFile() {
    return isRemoteFile;
  }

  public String getRemoteIp() {
    return remoteIp;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = getType(super.getOperatorType());
    stream.writeByte((byte) type);
    if (file == null) {
      stream.writeByte(0);
    } else {
      stream.writeByte(1);
      byte[] fileBytes = file.getPath().getBytes();
      stream.writeInt(fileBytes.length);
      stream.write(fileBytes);
    }

    if (targetDir == null) {
      stream.writeByte(0);
    } else {
      stream.writeByte(1);
      byte[] targetDirBytes = targetDir.getPath().getBytes();
      stream.writeInt(targetDirBytes.length);
      stream.write(targetDirBytes);
    }
    stream.writeBoolean(autoCreateSchema);
    stream.writeInt(sgLevel);
    stream.writeBoolean(isRemoteFile);
    if (isRemoteFile) {
      stream.writeInt(remoteIp.getBytes().length);
      stream.write(remoteIp.getBytes());
    }
  }

  //TODO
  private int getType(OperatorType operatorType) {
    switch (operatorType) {
      case LOAD_FILES:
        return PhysicalPlanType.LOAD_FILES.ordinal();
      default:
        return PhysicalPlanType.values().length + 1;
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = getType(super.getOperatorType());
    buffer.put((byte) type);
    if (file == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      putString(buffer, file.getPath());
    }

    if (targetDir == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      putString(buffer, targetDir.getPath());
    }

    if (autoCreateSchema) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
    }
    buffer.putInt(sgLevel);

    if (isRemoteFile) {
      buffer.put((byte) 1);
      putString(buffer, remoteIp);
    } else {
      buffer.put((byte) 0);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    if (buffer.get() == 1) {
      file = new File(readString(buffer));
    }

    if (buffer.get() == 1) {
      targetDir = new File(readString(buffer));
    }

    if (buffer.get() == 1) {
      autoCreateSchema = true;
    } else {
      autoCreateSchema = false;
    }
    sgLevel = buffer.getInt();

    if (buffer.get() == 1) {
      isRemoteFile = true;
    } else {
      isRemoteFile = false;
    }
    remoteIp = readString(buffer);
  }

  @Override
  public String toString() {
    return "OperateFilePlan{"
        + " file=" + file
        + ", targetDir=" + targetDir
        + ", autoCreateSchema=" + autoCreateSchema
        + ", sgLevel=" + sgLevel
        + ", isRemoteFile=" + isRemoteFile
        + ", remoteIp='" + remoteIp + '\''
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OperateFilePlan that = (OperateFilePlan) o;

    if (autoCreateSchema != that.autoCreateSchema) {
      return false;
    }
    if (sgLevel != that.sgLevel) {
      return false;
    }
    if (isRemoteFile != that.isRemoteFile) {
      return false;
    }
    if (!Objects.equals(file, that.file)) {
      return false;
    }
    if (!Objects.equals(targetDir, that.targetDir)) {
      return false;
    }
    return Objects.equals(remoteIp, that.remoteIp);
  }

  @Override
  public int hashCode() {
    int result = file != null ? file.hashCode() : 0;
    result = 31 * result + (targetDir != null ? targetDir.hashCode() : 0);
    result = 31 * result + (autoCreateSchema ? 1 : 0);
    result = 31 * result + sgLevel;
    result = 31 * result + (isRemoteFile ? 1 : 0);
    result = 31 * result + (remoteIp != null ? remoteIp.hashCode() : 0);
    return result;
  }
}
