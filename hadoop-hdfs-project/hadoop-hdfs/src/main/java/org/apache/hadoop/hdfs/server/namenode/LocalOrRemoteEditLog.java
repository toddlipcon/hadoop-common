package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface LocalOrRemoteEditLog {
  public InputStream getInputStream() throws IOException;
  public long length();
  public String getName();
  
  public static class FileLog implements LocalOrRemoteEditLog {
    private final File file;
    
    public FileLog(File file) {
      this.file = file;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return new FileInputStream(file);
    }

    @Override
    public long length() {
      return file.length();
    }

    @Override
    public String getName() {
      return file.getPath();
    }
    
    
  }
}
