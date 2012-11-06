package org.apache.hadoop.net.unix;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;

/**
 * Create a temporary directory in which sockets can be created.
 * When creating a UNIX domain socket, the name
 * must be fairly short (around 110 bytes on most platforms).
 *
 * @return
 */
public class TemporarySocketDirectory implements Closeable {
  private File dir;

  public TemporarySocketDirectory() {
    String tmp = System.getenv("TMPDIR");
    if (tmp == null) {
      tmp = "/tmp";
    }
    dir = new File(tmp, "socks." + (System.currentTimeMillis() +
        "." + (new Random().nextInt())));
    dir.mkdirs();
    dir.setWritable(true, true);
  }

  public File getDir() {
    return dir;
  }

  @Override
  public void close() throws IOException {
    if (dir != null) {
      FileUtils.deleteDirectory(dir);
      dir = null;
    }
  }

  protected void finalize() throws IOException {
    close();
  }
}
