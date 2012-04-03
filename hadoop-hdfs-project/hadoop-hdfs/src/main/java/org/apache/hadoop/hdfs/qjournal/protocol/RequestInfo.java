package org.apache.hadoop.hdfs.qjournal.protocol;

// TODO annotate
public class RequestInfo {
  private long epoch;
  private long ipcSerialNumber;
  
  public RequestInfo(long epoch, long ipcSerialNumber) {
    super();
    this.epoch = epoch;
    this.ipcSerialNumber = ipcSerialNumber;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  public long getIpcSerialNumber() {
    return ipcSerialNumber;
  }

  public void setIpcSerialNumber(long ipcSerialNumber) {
    this.ipcSerialNumber = ipcSerialNumber;
  }
}
