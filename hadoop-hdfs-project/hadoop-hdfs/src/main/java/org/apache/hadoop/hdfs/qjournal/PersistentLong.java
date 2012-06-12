package org.apache.hadoop.hdfs.qjournal;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.util.LongContainingFile;

class PersistentLong {
  private final File file;
  private final long defaultVal;
  
  private long value;
  private boolean loaded = false;
  
  public PersistentLong(File file, long defaultVal) {
    this.file = file;
    this.defaultVal = defaultVal;
  }
  
  public long get() throws IOException {
    if (!loaded) {
      value = LongContainingFile.read(file, defaultVal);
      loaded = true;
    }
    return value;
  }
  
  public void set(long newVal) throws IOException {
    LongContainingFile.write(file, newVal);
    value = newVal;
    loaded = true;
  }
}
