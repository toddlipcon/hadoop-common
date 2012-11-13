package org.apache.hadoop.net;

import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.hadoop.io.LongWritable;

public interface TransferToCapable {

  public void transferToFully(FileChannel fileCh, long position, int count,
      LongWritable waitForWritableTime,
      LongWritable transferToTime) throws IOException;

  public void write(byte[] buf, int off, int len) throws IOException;
}
