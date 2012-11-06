/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net.unix;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.NativeCodeLoader;

public class TestDomainSocket {
  private static TemporarySocketDirectory sockDir;

  @BeforeClass
  public static void init() {
    sockDir = new TemporarySocketDirectory();
  }

  @AfterClass
  public static void shutdown() throws IOException {
    sockDir.close();
  }
  
  /**
   * Test that we can create a socket and close it, even if it hasn't been
   * opened.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketCreateAndClose() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    DomainSocket client = new DomainSocket();
    client.close();
    ServerDomainSocket serv = new ServerDomainSocket();
    serv.close();
  }

  /**
   * Test DomainSocket path setting and getting.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketPathSetGet() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    DomainSocket client = new DomainSocket();
    client.setPath("/tmp/sock.%d");
    DomainSocketImpl impl = client.getDomainSocketImpl();
    final String EXPECTED_PATH = "/tmp/sock.100";
    String path = impl.getSetupPath(100);
    Assert.assertTrue("getPath returned " + path, EXPECTED_PATH.equals(path));
    client.setPath("/tmp/sock.%%d");
    final String EXPECTED_PATH2 = "/tmp/sock.%d";
    path = impl.getSetupPath(100);
    Assert.assertTrue("getPath returned " + path, EXPECTED_PATH2.equals(path));
  }

  /**
   * Test that if one thread is blocking in accept(), another thread
   * can close the socket and stop the accept.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testSocketAcceptAndClose() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    final int TEST_PORT = 1001;
    final ServerDomainSocket serv = new ServerDomainSocket();
    serv.setPath(new File(sockDir.getDir(), "test_sock.%d").getAbsolutePath());
    serv.bind(new InetSocketAddress(TEST_PORT));
    Thread accepter = new Thread() {
      public void run(){
        try {
          serv.accept();
          Assert.fail("expected the accept() to be interrupted and fail");
        } catch (IOException e) {
          // ignore
        }
      }
    };
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      // ignore
    }
    serv.close();
    try {
      accepter.join();
    } catch (InterruptedException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Test that operations on a closed socket don't work.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testInvalidOperations() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    DomainSocket client = new DomainSocket();
    try {
      client.getInputStream();
      Assert.fail("expected getInputStream() to fail on a " +
          "closed DomainSocket.");
    } catch (IOException e) {
      // ignore
    }
    try {
      client.getOutputStream();
      Assert.fail("expected getOutputStream() to fail on a " +
          "closed DomainSocket.");
    } catch (IOException e) {
      // ignore
    }
    try {
      client.connect(new InetSocketAddress(4040));
      Assert.fail("expected connect() to fail since a DomainSocket " +
          "path was not set.");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().startsWith("You must set a " +
          "DomainSocket path."));
    }
    final String TEST_PATH =
        new File(sockDir.getDir(), "test_sock.%d").getAbsolutePath();
    client.setPath(TEST_PATH);
    try {
      client.connect(new InetSocketAddress(4041));
      Assert.fail("expected connect() to fail since there's no " +
          "server to connect to.");
    } catch (IOException e) {
      // ignore
    }
    final ServerDomainSocket serv = new ServerDomainSocket();
    Assert.assertFalse(serv.isBound());
    Assert.assertEquals(null, serv.getChannel());
  }

  /**
   * Test setting some server options.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testServerOptions() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    final int TEST_PORT = 1003;
    final String TEST_PATH =
        new File(sockDir.getDir(), "test_sock.%d").getAbsolutePath();
    ServerDomainSocket serv = new ServerDomainSocket();
    try {
      serv.setPath(TEST_PATH);
      serv.bind(new InetSocketAddress(TEST_PORT));
      // Let's set a new receive buffer size
      int bufSize = serv.getReceiveBufferSize();
      int newBufSize = bufSize / 2;
      serv.setReceiveBufferSize(newBufSize);
      int nextBufSize = serv.getReceiveBufferSize();
      Assert.assertEquals(newBufSize, nextBufSize);
      // Let's set a server timeout
      int newTimeout = 1000;
      serv.setSoTimeout(newTimeout);
      int nextTimeout = serv.getSoTimeout();
      Assert.assertEquals(newTimeout, nextTimeout);
      try {
        serv.accept();
        Assert.fail("expected the accept() to time out and fail");
      } catch (IOException e) {
        // TODO: throw SocketTimeoutException instead of generic IOException
        // when the accept() times out.
        
        // ignore
      }
      
    } finally {
      serv.close();
      Assert.assertTrue(serv.isClosed());
    }
  }
  
  static class Success extends Throwable {
    private static final long serialVersionUID = 1L;
  }
  
  static interface WriteStrategy {
    /**
     * Initialize a WriteStrategy object from a Socket.
     */
    public void init(Socket s) throws IOException;
    
    /**
     * Write some bytes.
     */
    public void write(byte b[]) throws IOException;
  }
  
  static class OutputStreamWriteStrategy implements WriteStrategy {
    private OutputStream outs = null;
    
    public void init(Socket s) throws IOException {
      outs = s.getOutputStream();
    }
    
    public void write(byte b[]) throws IOException {
      outs.write(b);
    }
  }
  
  static class DirectByteBufferWriteStrategy implements WriteStrategy {
    private SocketChannel ch = null;
    
    public void init(Socket s) throws IOException {
      ch = s.getChannel();
    }
    
    public void write(byte b[]) throws IOException {
      ByteBuffer buf = ByteBuffer.allocateDirect(b.length);
      buf.put(b);
      buf.clear();
      ch.write(buf);
    }
  }

  static class ArrayBackedByteBufferWriteStrategy implements WriteStrategy {
    private SocketChannel ch = null;
    
    public void init(Socket s) throws IOException {
      ch = s.getChannel();
    }
    
    public void write(byte b[]) throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(b);
      buf.clear();
      ch.write(buf);
    }
  }
  
  abstract static class ReadStrategy {
    /**
     * Initialize a ReadStrategy object from a Socket.
     */
    public abstract void init(Socket s) throws IOException;
    
    /**
     * Read some bytes.
     */
    public abstract int read(byte b[], int off, int length) throws IOException;
    
    public void readFully(byte buf[], int off, int len) throws IOException {
      int toRead = len;
      while (toRead > 0) {
        int ret = read(buf, off, toRead);
        if (ret < 0) {
          throw new IOException( "Premature EOF from inputStream");
        }
        toRead -= ret;
        off += ret;
      }
    }
  }
  
  static class InputStreamReadStrategy extends ReadStrategy {
    private InputStream ins = null;
    
    @Override
    public void init(Socket s) throws IOException {
      ins = s.getInputStream();
    }

    @Override
    public int read(byte b[], int off, int length) throws IOException {
      return ins.read(b, off, length);
    }
  }
  
  static class DirectByteBufferReadStrategy extends ReadStrategy {
    private SocketChannel ch = null;
    
    @Override
    public void init(Socket s) throws IOException {
      ch = s.getChannel();
    }
    
    @Override
    public int read(byte b[], int off, int length) throws IOException {
      ByteBuffer buf = ByteBuffer.allocateDirect(b.length);
      int nread = ch.read(buf);
      if (nread < 0) return nread;
      buf.flip();
      buf.get(b, off, nread);
      return nread;
    }
  }

  static class ArrayBackedByteBufferReadStrategy extends ReadStrategy {
    private SocketChannel ch = null;
    
    @Override
    public void init(Socket s) throws IOException {
      ch = s.getChannel();
    }
    
    @Override
    public int read(byte b[], int off, int length) throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(b);
      int nread = ch.read(buf);
      if (nread < 0) return nread;
      buf.flip();
      buf.get(b, off, nread);
      return nread;
    }
  }
  
  /**
   * Test a simple client/server interaction.
   *
   * @throws IOException
   */
  void testClientServer1(final Class<? extends WriteStrategy> writeStrategyClass,
      final Class<? extends ReadStrategy> readStrategyClass) throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    final int TEST_PORT = 1002;
    final String TEST_PATH =
        new File(sockDir.getDir(), "test_sock.%d").getAbsolutePath();
    final byte clientMsg1[] = new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6 };
    final byte serverMsg1[] = new byte[] { 0x9, 0x8, 0x7, 0x6, 0x5 };
    final byte clientMsg2 = 0x45;
    final ArrayBlockingQueue<Throwable> threadResults =
        new ArrayBlockingQueue<Throwable>(2);
    final ServerDomainSocket serv = new ServerDomainSocket();
    final DomainSocket client = new DomainSocket();
    try {
      serv.setPath(TEST_PATH);
      Assert.assertEquals(-1, serv.getLocalPort());
      serv.bind(new InetSocketAddress(TEST_PORT));
      Assert.assertEquals(TEST_PORT, serv.getLocalPort());

      Thread serverThread = new Thread() {
        public void run(){
          // Run server
          Socket conn = null;
          try {
            conn = serv.accept();
            byte in1[] = new byte[clientMsg1.length];
            ReadStrategy reader = readStrategyClass.newInstance();
            reader.init(conn);
            reader.readFully(in1, 0, in1.length);
            Assert.assertTrue(Arrays.equals(clientMsg1, in1));
            WriteStrategy writer = writeStrategyClass.newInstance();
            writer.init(conn);
            writer.write(serverMsg1);
            InputStream connInputStream = conn.getInputStream();
            int in2 = connInputStream.read();
            Assert.assertEquals((int)clientMsg2, in2);
            conn.close();
          } catch (Throwable e) {
            threadResults.add(e);
            Assert.fail(e.getMessage());
          }
          threadResults.add(new Success());
        }
      };
      serverThread.start();
      
      Thread clientThread = new Thread() {
        public void run(){
          try {
            client.setPath(TEST_PATH);
            client.connect(new InetSocketAddress(TEST_PORT));
            WriteStrategy writer = writeStrategyClass.newInstance();
            writer.init(client);
            writer.write(clientMsg1);
            ReadStrategy reader = readStrategyClass.newInstance();
            reader.init(client);
            byte in1[] = new byte[serverMsg1.length];
            reader.readFully(in1, 0, in1.length);
            Assert.assertTrue(Arrays.equals(serverMsg1, in1));
            OutputStream clientOutputStream = client.getOutputStream();
            clientOutputStream.write(clientMsg2);
          } catch (Throwable e) {
            threadResults.add(e);
            Assert.fail(e.getMessage());
          }
          threadResults.add(new Success());
        }
      };
      clientThread.start();
      
      for (int i = 0; i < 2; i++) {
        Throwable t = threadResults.take();
        if (!(t instanceof Success)) {
          Assert.fail(t.getMessage() + ExceptionUtils.getStackTrace(t));
        }
      }
      serverThread.join();
      clientThread.join();
    } catch (InterruptedException e) {
      Assert.fail(e.getMessage());
    }
    serv.close();
    client.close();
  }

  @Test(timeout=180000)
  public void testClientServerWithStreams() throws IOException {
    testClientServer1(OutputStreamWriteStrategy.class,
        InputStreamReadStrategy.class);
  }

  @Test(timeout=180000)
  public void testClientServerWithDirectByteBuffers() throws IOException {
    testClientServer1(DirectByteBufferWriteStrategy.class,
        DirectByteBufferReadStrategy.class);
  }

  @Test(timeout=180000)
  public void testClientServerWithIndirectByteBuffers() throws IOException {
    testClientServer1(ArrayBackedByteBufferWriteStrategy.class,
        ArrayBackedByteBufferReadStrategy.class);
  }

  @Test(timeout=180000)
  public void testClientServerWithMixedStrategies() throws IOException {
    testClientServer1(OutputStreamWriteStrategy.class,
        DirectByteBufferReadStrategy.class);
  }

  static private class PassedFile {
    private final int idx;
    private final byte[] contents;
    private FileInputStream fis;
    
    public PassedFile(int idx) throws IOException {
      this.idx = idx;
      this.contents = new byte[] { (byte)(idx % 127) };
      new File(getPath()).delete();
      FileOutputStream fos = new FileOutputStream(getPath(), false);
      try {
        fos.write(contents);
      } finally {
        fos.close();
      }
      this.fis = new FileInputStream(getPath());
    }

    public String getPath() {
      return new File(sockDir.getDir(), "passed_file" + idx).getAbsolutePath();
    }

    public FileInputStream getInputStream() throws IOException {
      return fis;
    }
    
    public void cleanup() throws IOException {
      new File(getPath()).delete();
      fis.close();
    }

    public void checkInputStream(FileInputStream fis) throws IOException {
      byte buf[] = new byte[contents.length];
      IOUtils.readFully(fis, buf, 0, buf.length);
      Arrays.equals(contents, buf);
    }
    
    protected void finalize() {
      try {
        cleanup();
      } catch(Throwable t) {
        // ignore
      }
    }
  }

  /**
   * Test file descriptor passing.
   *
   * @throws IOException
   */
  @Test(timeout=180000)
  public void testFdPassing() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    final int TEST_PORT = 1005;
    final String TEST_PATH =
        new File(sockDir.getDir(), "test_sock.%d").getAbsolutePath();
    final byte clientMsg1[] = new byte[] { 0x11, 0x22, 0x33, 0x44, 0x55, 0x66 };
    final byte serverMsg1[] = new byte[] { 0x31, 0x30, 0x32, 0x34, 0x31, 0x33,
          0x44, 0x1, 0x1, 0x1, 0x1, 0x1 };
    final ArrayBlockingQueue<Throwable> threadResults =
        new ArrayBlockingQueue<Throwable>(2);
    final ServerDomainSocket serv = new ServerDomainSocket();
    final DomainSocket client = new DomainSocket();
    final PassedFile passedFiles[] =
        new PassedFile[] { new PassedFile(1), new PassedFile(2) };
    final FileDescriptor passedFds[] = new FileDescriptor[passedFiles.length];
    for (int i = 0; i < passedFiles.length; i++) {
      passedFds[i] = passedFiles[i].getInputStream().getFD();
    }

    try {
      serv.setPath(TEST_PATH);
      serv.bind(new InetSocketAddress(TEST_PORT));

      Thread serverThread = new Thread() {
        public void run(){
          // Run server
          Socket conn = null;
          try {
            conn = serv.accept();
            byte in1[] = new byte[clientMsg1.length];
            InputStream connInputStream = conn.getInputStream();
            IOUtils.readFully(connInputStream, in1, 0, in1.length);
            Assert.assertTrue(Arrays.equals(clientMsg1, in1));
            DomainSocket domainConn = (DomainSocket)conn;
            domainConn.sendFileDescriptors(passedFds, serverMsg1, 0,
                serverMsg1.length);
            conn.close();
          } catch (Throwable e) {
            threadResults.add(e);
            Assert.fail(e.getMessage());
          }
          threadResults.add(new Success());
        }
      };
      serverThread.start();

      Thread clientThread = new Thread() {
        public void run(){
          try {
            client.setPath(TEST_PATH);
            client.connect(new InetSocketAddress(TEST_PORT));
            OutputStream clientOutputStream = client.getOutputStream();
            InputStream clientInputStream = client.getInputStream();
            clientOutputStream.write(clientMsg1);
            DomainSocket domainConn = (DomainSocket)client;
            byte in1[] = new byte[serverMsg1.length];
            FileInputStream recvFis[] = new FileInputStream[passedFds.length];
            int r = domainConn.
                recvFileInputStreams(recvFis, in1, 0, in1.length - 1);
            Assert.assertTrue(r > 0);
            IOUtils.readFully(clientInputStream, in1, r, in1.length - r);
            Assert.assertTrue(Arrays.equals(serverMsg1, in1));
            for (int i = 0; i < passedFds.length; i++) {
              Assert.assertNotNull(recvFis[i]);
              passedFiles[i].checkInputStream(recvFis[i]);
            }
            for (FileInputStream fis : recvFis) {
              fis.close();
            }
          } catch (Throwable e) {
            threadResults.add(e);
            Assert.fail(e.getMessage());
          }
          threadResults.add(new Success());
        }
      };
      clientThread.start();
      
      for (int i = 0; i < 2; i++) {
        Throwable t = threadResults.take();
        if (!(t instanceof Success)) {
          Assert.fail(t.getMessage() + ExceptionUtils.getStackTrace(t));
        }
      }
      serverThread.join();
      clientThread.join();
    } catch (InterruptedException e) {
      Assert.fail(e.getMessage());
    }
    serv.close();
    client.close();
    for (PassedFile pf : passedFiles) {
      pf.cleanup();
    }
  }
  
  // TODO: test close/shutdown while client reads... do we get what we expect?
  // (i.e., a return of -1 / short read?)
  
  // TODO: does SocketChannel get the exceptions it wants?
  
  // EOF in general is pretty confusing; are we doing this right?
  
  // TODO: add getPort, getHost, etc test to connected / accept'ed socket
}
