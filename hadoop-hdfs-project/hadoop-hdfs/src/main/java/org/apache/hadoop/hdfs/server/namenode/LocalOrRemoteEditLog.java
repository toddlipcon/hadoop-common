package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.hdfs.server.namenode.TransferFsImage.HttpGetFailedException;
import org.apache.hadoop.security.SecurityUtil;

import com.google.common.base.Preconditions;

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
  
  public static class URLLog implements LocalOrRemoteEditLog {
    private final URL url;
    private long advertisedSize = -1;

    private final static String CONTENT_LENGTH = "Content-Length";

    public URLLog(URL url) {
      this.url = url;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      HttpURLConnection connection = (HttpURLConnection)
          SecurityUtil.openSecureHttpConnection(url);
      
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new HttpGetFailedException(
            "Image transfer servlet at " + url +
            " failed with status code " + connection.getResponseCode() +
            "\nResponse message:\n" + connection.getResponseMessage(),
            connection);
      }

      String contentLength = connection.getHeaderField(CONTENT_LENGTH);
      if (contentLength != null) {
        advertisedSize = Long.parseLong(contentLength);
      } else {
        throw new IOException(CONTENT_LENGTH + " header is not provided " +
                              "by the server when trying to fetch " + url);
      }

      return connection.getInputStream();
    }

    @Override
    public long length() {
      Preconditions.checkState(advertisedSize != -1,
          "must get input stream before length is available");
      return 0;
    }

    @Override
    public String getName() {
      return url.toString();
    }
  }
}
