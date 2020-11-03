#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM openjdk:8
RUN apt update \
  # procps is for `free` command
  && apt install wget unzip lsof procps -y \
  # && wget https://www-us.apache.org/dist/incubator/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-bin.zip \
  # if you are in China, use the following URL
  
  && wget http://172.16.125.72:8000/apache-iotdb-0.10.1-incubating-bin.zip \
  # && wget http://mirrors.tuna.tsinghua.edu.cn/apache/incubator/iotdb/0.10.1-incubating/apache-iotdb-0.10.1-incubating-bin.zip \
  && unzip apache-iotdb-0.10.1-incubating-bin.zip \
  && rm apache-iotdb-0.10.1-incubating-bin.zip \
  && mv apache-iotdb-0.10.1-incubating /iotdb \
  && apt remove wget unzip -y \
  && apt autoremove -y \
  && apt purge --auto-remove -y \
  && apt clean -y
EXPOSE 6667
EXPOSE 31999
EXPOSE 5555
EXPOSE 8181
VOLUME /iotdb/
VOLUME /iotdb/data
VOLUME /iotdb/logs
ENV PATH="/iotdb/sbin/:/iotdb/tools/:${PATH}"
ARG params=""
ENV params ${params}
RUN echo ${params}
CMD ["/bin/sh","-c","/iotdb/sbin/start-server.sh ${params} "]
