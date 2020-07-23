package org.apache.iotdb.cluster.utils;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author HouliangQi(neuyilan @ 163.com)
 * @description
 * @since 2020-07-23 9:59 上午
 */
public class ReentrantLockClass extends ReentrantLock {

  public String owner() {
    Thread t = this.getOwner();
    if (t != null) {
      return t.getName();
    } else {
      return "none";
    }
  }
}
