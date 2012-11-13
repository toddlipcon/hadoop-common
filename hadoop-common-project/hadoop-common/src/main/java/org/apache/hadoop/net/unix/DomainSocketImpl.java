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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.net.TransferToCapable;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.base.Preconditions;

/**
 * The implementation of UNIX domain sockets in Java.
 * 
 * See {@link DomainSocket} for more information about UNIX domain sockets.
 */
public class DomainSocketImpl extends SocketImpl {
  static Log LOG = LogFactory.getLog(DomainSocketImpl.class);
            
  static final InetAddress LOOPBACK_ADDR;

  static {
    try {
      LOOPBACK_ADDR = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
    } catch (UnknownHostException e) {
      throw new RuntimeException("unable to resolve the loopback address", e);
    }
  }

  /**
   * Initialize the native library code.
   */
  private static native void anchorNative();

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      anchorNative();
    }
  }
  
  native DomainSocket getDomainSocket0();
  
  private static final long FDINFO_CLOSED_UNREFERENCED = 0x7fffffff00000000L;

  /**
   * File descriptor and reference count.
   *
   * If a file descriptor is closed, and another thread uses that same file
   * descriptor, the second thread may end up performing an operation on a
   * file other than the intended one.  To prevent this, we increment the
   * reference count on the file descriptor while using it, and only close a
   * file descriptor after its reference count drops to 0.
   *
   * Top 32 bits: file descriptor.  If -1, the fd is closed.
   * Bottom 32 bits: reference count of file descriptor.  If 0, there are no
   * threads using the file descriptor.  If >0, this is the number of threads
   * using the file descriptor.
   */
  private final AtomicLong fdInfo = new AtomicLong(FDINFO_CLOSED_UNREFERENCED);

  /**
   * Atomically get the current file descriptor and increase the reference
   * count of the current file descriptor.
   * 
   * @return                 The current file descriptor
   * @throws IOException     If the current file descriptor is closed
   */
  private int fdRef() throws IOException {
    long info = fdInfo.incrementAndGet();
    int fd = (int)((info >> 32) & 0xffffffff);
    if (fd < 0) {
      fdInfo.decrementAndGet();
      throw new IOException("The socket is closed.");
    }
    return fd;
  }

  /**
   * Return true if the file descriptor is currently closed.
   * This function is just for sanity checking.  Its return value may be
   * invalid by the time you read it.
   * 
   * @return                 True if the file descriptor is currently closed.
   */
  private boolean isClosed() {
    long info = fdInfo.get();
    int fd = (int)((info >> 32) & 0xffffffff);
    return (fd < 0);
  }
  
  /**
   * Atomically release the current file descriptor.
   */
  private void fdUnref() {
    fdInfo.decrementAndGet();
  }

  /**
   * Try to atomically change the file descriptor from the unused fd
   * to a new, valid file descriptor.
   * 
   * @param fd               The new file descriptor
   * @return                 True if the change was made; false otherwise.
   *                         This will fail if the file descriptor is
   *                         currently set to something other than -1, or
   *                         if someone is referencing the current fdinfo.
   */
  private boolean setFd(int fd) {
    long info = ((long)fd) << 32;
    return fdInfo.compareAndSet(FDINFO_CLOSED_UNREFERENCED, info);
  }

  /**
   * The path to use during setup.
   * See {@link DomainSocketImpl#getPath(int)} for details.
   *
   * Locking: protected by the object monitor.
   */
  private String setupPath = null;

  /**
   * The path we used to bind() to the socket, or null if we never called
   * bind().
   *
   * Locking: protected by the object monitor.
   */
  private String bindPath = null;
  
  /**
   * The InputStream associated with this socket.
   *
   * Locking: protected by the object monitor.
   */
  private SocketInputStream socketInputStream = null;

  /**
   * The OutputStream associated with this socket.
   *
   * Locking: protected by the object monitor.
   */
  private SocketOutputStream socketOutputStream = null;

  public DomainSocketImpl() {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      throw new UnsupportedOperationException("Can't create " +
          "DomainSocketImpl: libhadoop.so has not been loaded.");
    }
  }

  private static native void setTimeout0(int fd, int timeo,
      boolean setSendTimeout, boolean setRecvTimeout) throws IOException;

  void setTimeouts(int timeo, boolean setSendTimeout, 
                   boolean setRecvTimeout) throws IOException {
    int fd;
    try {
      fd = fdRef();
      setTimeout0(fd, timeo, setSendTimeout, setRecvTimeout);
    } finally {
      fdUnref();
    }
  }

  private static native int getTimeout0(int fd) throws IOException;

  private static native void setBufSize0(int fd, boolean isSnd, int bufSize)
      throws IOException;

  private static native int getBufSize0(int fd, boolean isSnd)
      throws IOException;

  /**
   * Get the path to use for this UNIX domain socket.
   *
   * If the escape sequence '%d' appears in the configured socket path,
   * we will replace this with the port number.  To get a literal '%d',
   * use '%%d'.
   *
   * This functionality is provided to make configuration easier for the
   * case where you have several daemons on the same system with the same
   * configuration.
   * 
   * @param port      The port number to use.
   * @return          The socket path.
   */
  synchronized String getSetupPath(int port) {
    if (setupPath == null) {
      // Let's throw something a little more developer-friendly than an NPE.
      throw new RuntimeException("You must set a DomainSocket path.");
    }
    return getEffectivePath(setupPath, port);
  }
  
  public static String getEffectivePath(String path, int port) {
    StringBuilder bld = new StringBuilder();
    char prevChar = ' ';
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%') {
        if (prevChar == '%') {
          bld.append('%');
          prevChar = ' ';
        } else {
          prevChar = '%';
        }
      } else if ((c == 'd') && (prevChar == '%')) {
        bld.append(port);
        prevChar = c;
      } else {
        bld.append(c);
        prevChar = c;
      }
    }
    return bld.toString();
  }

  /**
   * Set the setupPath.
   * 
   * @param setupPath        The path.
   */
  public synchronized void setSetupPath(String setupPath) {
    this.setupPath = setupPath;
  }

  /**
   * Set the bindPath.
   * 
   * @param bindPath         The path.
   */
  private synchronized void setBindPath(String bindPath) {
    this.bindPath = bindPath;
  }

  /**
   * Get the bindPath.
   */
  public synchronized String getBindPath() {
    return this.bindPath;
  }

  @Override
  public void setOption(int optID, Object value)
      throws SocketException {
    int fd;
    try {
      fd = fdRef();
    } catch (IOException e) {
      throw new SocketException(e.getMessage());
    }
    try {
      switch (optID) {
      case SO_TIMEOUT:
        try {
          setTimeout0(fd, ((Number)value).intValue(), false, true);
        } catch (IOException e) {
          throw new SocketException(e.getMessage());
        }
        break;
      case SO_SNDBUF:
        try {
          setBufSize0(fd, true, ((Number)value).intValue());
        } catch (IOException e) {
          throw new SocketException(e.getMessage());
        }
        break;
      case SO_RCVBUF:
        try {
          setBufSize0(fd, false, ((Number)value).intValue());
        } catch (IOException e) {
          throw new SocketException(e.getMessage());
        }
        break;
      default:
        throw new SocketException("Option " + optID + " is not supported " +
            "for UNIX domain sockets.");
      }
    } finally {
      fdUnref();
    }
  }

  @Override
  public Object getOption(int optID) throws SocketException {
    int fd;
    try {
      fd = fdRef();
    } catch (IOException e) {
      throw new SocketException(e.getMessage());
    }
    try {
      switch (optID) {
        case SO_BINDADDR:
          return address;
        case SO_TIMEOUT:
          try {
            Integer i = Integer.valueOf(getTimeout0(fd));
            return i;
          } catch (IOException e) {
            throw new SocketException(e.getMessage());
          }
        case SO_SNDBUF:
          try {
            return Integer.valueOf(getBufSize0(fd, true));
          } catch (IOException e) {
            throw new SocketException(e.getMessage());
          }
        case SO_RCVBUF:
          try {
            return Integer.valueOf(getBufSize0(fd, false));
          } catch (IOException e) {
            throw new SocketException(e.getMessage());
          }
        default:
          throw new SocketException("Option " + optID + " is not supported " +
                "for UNIX domain sockets.");
      }
    } finally {
      fdUnref();
    }
  }

  /**
   * Create a socket.
   * 
   * @param stream             If true, create a stream socket; if 
   *                           false, a packet socket.
   * @return                   The file descriptor of the socket.
   * @throws IOException       If socket() fails.
   */
  private static native int create0(boolean stream) throws IOException;

  /**
   * Create a socket.
   * 
   * @param stream             If true, create a stream socket; if 
   *                           false, a packet socket.
   * @throws IOException       If socket() fails.
   */
  @Override
  protected void create(boolean stream) throws IOException {
    int fd = create0(stream);
    if (!setFd(fd)) {
      try {
        close0(fd);
      } catch (Throwable e) {
        LOG.error("create: error closing just-opened socket", e);
      }
      throw new IOException("DomainSocketImpl#create " +
          "must be called on an unused SocketImpl, but this one is in use.");
    }
  }

  /**
   * Setup a socket.
   *
   * @param fd               File descriptor of socket to set up.
   * @param path             The socket path to use.
   * @param doConnect        If true-- call connect().  If false-- call bind().
   */
  private native void setup0(int fd, String path,
      boolean doConnect) throws IOException;

  /**
   * Setup a socket.
   * 
   * Note that when binding to a path, we'll try to unlink the socket if it
   * already exists.
   *
   * @param path             The socket path to use.
   * @param doConnect        If true-- call connect().  If false-- call bind().
   * @param newPort          Port number to set.
   */
  private void setup(String path, boolean doConnect, int newPort)
      throws IOException {
    int fd = fdRef();
    try {
      setup0(fd, path, doConnect);
      synchronized(this) {
        // set the address, port, and localport to something reasonable
        address = LOOPBACK_ADDR;
        if (doConnect) {
          port = newPort;
          localport = 0xffff - port;
        } else {
          port = 0;
          localport = newPort;
        }
      }
    } finally {
      fdUnref();
    }
  }

  /**
   * Connect to a server listening on a UNIX domain socket.
   * 
   * @param host             This parameter will be ignored (all UNIX domain 
   *                         sockets are local).
   * @param connPort         This parameter will used to calculate the effective
   *                         socket path.
   */
  @Override
  protected void connect(String host, int connPort)
      throws IOException {
    setup(getSetupPath(connPort), true, connPort);
  }

  /**
   * Connect to a server listening on a UNIX domain socket.
   * See {@link DomainSocketImpl#connect(String, int)}.
   */
  @Override
  protected void connect(InetAddress address, int connPort)
      throws IOException {
    setup(getSetupPath(connPort), true, connPort);
  }

  /**
   * Connect to a server listening on a UNIX domain socket.
   * See {@link DomainSocketImpl#connect(String, int)}.
   */
  @Override
  protected void connect(SocketAddress address, int timeout)
      throws IOException {
    int connPort = 0;
    if (address instanceof InetSocketAddress) {
      InetSocketAddress iAddress = (InetSocketAddress)address;
      connPort = iAddress.getPort();
    }
    setup(getSetupPath(connPort), true, connPort);
  }

  /**
   * Bind to a UNIX domain socket.
   * 
   * @param host             This parameter will be ignored (all UNIX domain 
   *                         sockets are local).
   * @param bindPort         This parameter will used to calculate the effective
   *                         socket setup path.
   *                         See {@link DomainSocketImpl#getSetupPath(int)} for
   *                         details.
   */
  @Override
  protected void bind(InetAddress host, int bindPort)
      throws IOException {
    setup(getSetupPath(bindPort), false, bindPort);
  }

  /**
   * Listen on a UNIX domain socket.
   * 
   * @param fd               The file descriptor to listen on.
   * @param backlog          The connection backlog to use.
   */
  private static native void listen0(int fd, int backlog) throws IOException;

  /**
   * Listen on a UNIX domain socket.
   * 
   * You must bind to a socket before listening on it.
   * 
   * @param backlog          The connection backlog to use.
   */
  @Override
  protected void listen(int backlog) throws IOException {
    int fd = fdRef();
    try {
      listen0(fd, backlog);
    } finally {
      fdUnref();
    }
  }

  /**
   * Accept a connection on a UNIX domain socket.
   *
   * @param fd              File descriptor to accept() on.
   * @param newSock         The SocketImpl object to use for the
   *                        incoming socket.  We will put the path
   *                        to the new socket into newSock#path, and the
   *                        new file descriptor into newSocket#fdInfo.
   * @throws IOException    On accept() error.
   */
  private static native void accept0(int fd, DomainSocketImpl newSock)
      throws IOException;

  /**
   * Accept a connection on a UNIX domain socket.
   * 
   * You must listen on a socket before accepting a connection on it.
   *
   * This method will block until one of the following is true:
   * 1. the socket timeout that you set on this socket elapses
   * 2. the socket is closed by another thread
   * using {@link DomainSocketImpl#close()}
   * 3. You get an incoming connection.
   * 
   * @param newSock         The SocketImpl object to use for the
   *                        incoming socket
   */
  @Override
  protected void accept(SocketImpl newSock) throws IOException {
    if (!(newSock instanceof DomainSocketImpl)) {
      throw new RuntimeException("The argument to DomainSocketImpl#accept " +
          "must be a DomainSocketImpl, not some other kind of SocketImpl.");
    }
    DomainSocketImpl other = (DomainSocketImpl)newSock;
    int fd = fdRef();
    try {
      accept0(fd, other);
      synchronized (other) {
        other.address = LOOPBACK_ADDR;
        other.port = this.localport;
        other.localport = 0xffff - this.localport;
      }
    } finally {
      fdUnref();
    }
  }

  @Override
  protected synchronized InputStream getInputStream() throws IOException {
    if (isClosed()) {
      throw new IOException("Socket Closed");
    }
    if (socketInputStream != null) {
      return socketInputStream;
    }
    socketInputStream = new SocketInputStream();
    return socketInputStream;
  }

  @Override
  protected synchronized OutputStream getOutputStream() throws IOException {
    if (isClosed()) {
      throw new IOException("Socket Closed");
    }
    if (socketOutputStream != null) {
      return socketOutputStream;
    }
    socketOutputStream = new SocketOutputStream();
    return socketOutputStream;
  }

  private static native int available0(int fd) throws IOException;

  @Override
  public int available() throws IOException {
    int fd = fdRef();
    try {
      return available0(fd);
    } finally {
      fdUnref();
    }
  }
  
  /**
   * Close a file descriptor.
   * 
   * @param fd        The raw file descriptor to close
   */
  private static native void close0(int fd) throws IOException;

  /**
   * Closes the DomainSocketImpl.
   */
  @Override
  public void close() throws IOException {
    boolean didShutdown = false;
    while (true) {
      long info = fdInfo.get();
      int fd = (int)((info >> 32) & 0xffffffff);
      if (fd < 0) {
        return; // the socket is already closed.
      }
      int refCnt = (int)(info & 0xffffffff);
      if (refCnt == 0) {
        // Let's try to close the socket.
        if (fdInfo.compareAndSet(info, FDINFO_CLOSED_UNREFERENCED)) {
          String prevBindPath;
          synchronized(this) {
            prevBindPath = bindPath;
            setupPath = null;
            bindPath = null;
            socketInputStream = null;
            socketOutputStream = null;
            address = null;
            localport = port = 0;
          }
          // We now know that no other threads hold a reference to the socket.
          // It's time to tell UNIX to close it.
          // This may throw an exception, but that is ok... we have already
          // cleared the DomainSocketImpl state.
          close0(fd);
          // If we were a server socket bound to a socket file, 
          // clean up after ourselves by unlinking the now-useless file.
          if (prevBindPath != null) {
            if (new File(prevBindPath).delete()) {
              LOG.debug("failed to delete " + prevBindPath);
            }
          }
          return;
        }
      }
      if (!didShutdown) {
        // OK, so someone is using the socket currently.
        // If we haven't invoked shutdown() on the socket, let's do that now.
        // This will allow threads to break out of calls to accept() and
        // connect().
        try {
          shutdown0(fd, true, true);
        } catch (Throwable e) {
          LOG.error("Error shutting down socket.", e);
        }
        didShutdown = true;
      }
      try {
        // Busy-wait for anyone holding a reference count to the socket to
        // give up on it.
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }
  
  /**
   * Clean up if the user forgets to close the socket.
   */
  protected void finalize() throws IOException {
  	close();
  }

  private static native void shutdown0(int fd, boolean shutdownInput,
      boolean shutdownOutput) throws IOException;

  @Override
  protected void shutdownInput() throws IOException {
    int fd = fdRef();
    try {
      shutdown0(fd, true, false);
    } finally {
      fdUnref();
    }
  }

  @Override
  protected void shutdownOutput() throws IOException {
    int fd = fdRef();
    try {
      shutdown0(fd, false, true);
    } finally {
      fdUnref();
    }
  }

  @Override
  protected boolean supportsUrgentData () {
    return false;
  }
  
  @Override
  protected void sendUrgentData(int data) throws IOException {
    throw new IOException("UNIX domain sockets do not support urgent data");
  }
  
  @Override
  protected void setPerformancePreferences(int connectionTime,
                                    int latency, int bandwidth) {
    // does nothing
  }

  /**
   * Send some FileDescriptor objects to the process on the other side of this
   * socket.
   * 
   * See {@link DomainSocket#sendFileDescriptors(org.apache.hadoop.net.unix.FileDescriptor[], ByteBuffer)}
   */
  private static native void sendFileDescriptors0(int fd, FileDescriptor jfds[],
      byte jbuf[], int offset, int length) throws IOException;

  /**
   * Send some FileDescriptor objects to the process on the other side of this
   * socket.
   * 
   * See {@link DomainSocket#sendFileDescriptors}
   */
  public void sendFileDescriptors(FileDescriptor jfds[], byte jbuf[],
      int offset, int length) throws IOException {
    int fd = fdRef();
    try {
      sendFileDescriptors0(fd, jfds, jbuf, offset, length);
    } finally {
      fdUnref();
    }
  }

  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket.
   *
   * See {@link DomainSocketImpl#recvFileDescriptors(FileDescriptor[], byte[], int, int)}
   */
  private static native int recvFileDescriptors0(int fd, FileDescriptor[] jfds,
      byte jbuf[], int offset, int length) throws IOException;

  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket.
   *
   * @param jfds              (output parameter) Array of FileDescriptors.
   *                          We will fill as many slots as possible with file
   *                          descriptors passed from the remote process.  The
   *                          other slots will contain NULL.
   * @param jbuf              (output parameter) Buffer to read into.
   *                          The UNIX domain sockets API requires you to read
   *                          at least one byte from the remote process, even
   *                          if all you care about is the file descriptors
   *                          you will receive.
   * @param offset            Offset into the byte buffer to load data
   * @param length            Length of the byte buffer to use for data
   *
   * @return                  The number of bytes read.  Always a positive
   *                          number.
   * @throws                  EOFException on EOF.
   *                          IOException on another I/O exception.
   */
  public int recvFileDescriptors(FileDescriptor[] jfds,
      byte jbuf[], int offset, int length) throws IOException {
    int fd = fdRef();
    try {
      return recvFileDescriptors0(fd, jfds, jbuf, offset, length);
    } finally {
      fdUnref();
    }
  }
  
  /**
   * Close a FileDescriptor.
   *
   * Note: you should not close a FileDescriptor which you have used to create a
   * FileInputStream or FileOutputStream.  Similarly, closing a FileDescriptor
   * which has already been closed will result in undefined behavior.
   *
   * @param fd                The FileDescriptor to close.
   * @throws                  Throws no exceptions.
   */
  private static native void closeFileDescriptor0(FileDescriptor fd);

  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket, and wrap them in FileInputStream objects.
   *
   * See {@link DomainSocket#recvFileInputStreams(ByteBuffer)}
   */
  int recvFileInputStreams(FileInputStream[] fis, byte buf[],
        int offset, int length) throws IOException {
    FileDescriptor fds[] = new FileDescriptor[fis.length];
    boolean success = false;
    for (int i = 0; i < fis.length; i++) {
      fis[i] = null;
    }
    try {
      int ret = recvFileDescriptors(fds, buf, offset, length);
      for (int i = 0, j = 0; i < fds.length; i++) {
        if (fds[i] != null) {
          fis[j++] = new FileInputStream(fds[i]);
          fds[i] = null;
        }
      }
      success = true;
      return ret;
    } finally {
      if (!success) {
        for (int i = 0; i < fds.length; i++) {
          if (fds[i] != null) {
            closeFileDescriptor0(fds[i]);
          } else if (fis[i] != null) {
            try {
              fis[i].close();
            } catch (Throwable t) {
              LOG.error(t);
            } finally {
              fis[i] = null; }
          }
        }
      }
    }
  }
  
  static native int read0(int fd) throws IOException;
  
  static native int readArray0(int fd, byte b[], int off, int len)
      throws IOException;

  static native int readByteBuffer0(int fd, long addr, int len) throws IOException;
  
  int readByteBuffer(ByteBuffer dst) throws IOException {
    int fd = fdRef();
    try {
      return readByteBufferInternal(fd, dst);
    } finally {
      fdUnref();
    }
  }

  private int readByteBufferInternal(int fd, ByteBuffer dst) throws IOException {
    Preconditions.checkArgument(dst.isDirect());
    // TODO support non-direct using a local tmp buffer
    int pos = dst.position();
    int rem = dst.remaining();
    long addr = ((sun.nio.ch.DirectBuffer)dst).address() + pos;
    int n = readByteBuffer0(fd, addr, rem);
    if (n > 0) {
      dst.position(pos + n);
    }
    return n;
  }

  long readByteBufferArray(ByteBuffer[] dsts, int offset, int length)
      throws IOException {
    long total = 0;
    int fd = fdRef();
    try {
      for (int i = offset; i < length; i++) {
        int nread = readByteBufferInternal(fd, dsts[i]);
        total += nread;
        if (dsts[i].remaining() > 0) {
          break;
        }
      }
    } finally {
      fdUnref();
    }
    return total;
  }

  /**
   * Input stream for UNIX domain sockets.
   */
  public class SocketInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      int fd = fdRef();
      try {
        return read0(fd);
      } finally {
        fdUnref();
      }
    }
    
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      int fd = fdRef();
      try {
        return readArray0(fd, b, off, len);
      } finally {
        fdUnref();
      }
    }

    @Override
    public int available() throws IOException {
      return DomainSocketImpl.this.available();
    }
    
    @Override
    public void close() throws IOException {
      DomainSocketImpl.this.close();
    }
    
    public int recvFileInputStreams(FileInputStream[] fis, byte buf[],
          int offset, int length) throws IOException {
      return DomainSocketImpl.this.
          recvFileInputStreams(fis, buf, offset, length);
    }

    public DomainSocket getDomainSocket() {
      return DomainSocketImpl.this.getDomainSocket0();
    }
  }
  
  private static native void write0(int fd, int b) throws IOException;

  private static native void writeArray0(int fd, byte b[], int offset, int length)
      throws IOException;
  
  static native int writeByteBuffer0(int fd, ByteBuffer src) throws IOException;
  
  private static native int transferTo0(int fd, FileDescriptor srcFd,
      long offset, int length);

  int writeByteBuffer(ByteBuffer src) throws IOException {
    int fd = fdRef();
    try {
      return writeByteBuffer0(fd, src);
    } finally {
      fdUnref();
    }
  }

  long writeByteBufferArray(ByteBuffer[] srcs, int offset, int length)
      throws IOException {
    long total = 0;
    int fd = fdRef();
    try {
      for (int i = offset; i < length; i++) {
        int nwritten = writeByteBuffer0(fd, srcs[i]);
        total += nwritten;
        if (srcs[i].remaining() > 0) {
          break;
        }
      }
    } finally {
      fdUnref();
    }
    return total;
  }

  static Field fdField;
  static {
    try {
      fdField = sun.nio.ch.FileChannelImpl.class.getDeclaredField("fd");
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    fdField.setAccessible(true);
  }
  
  /**
   * Output stream for UNIX domain sockets.
   */
  public class SocketOutputStream extends OutputStream implements TransferToCapable {
    
    @Override
    public void close() throws IOException {
      DomainSocketImpl.this.close();
    }
    
    @Override
    public void write(int b) throws IOException {
      int fd = fdRef();
      try {
        write0(fd, b);
      } finally {
        fdUnref();
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      int fd = fdRef();
      try {
        writeArray0(fd, b, off, len);
      } finally {
        fdUnref();
      }
    }

    public void sendFileDescriptors(FileDescriptor fds[], byte buf[],
        int offset, int length) throws IOException {
      DomainSocketImpl.this.
          sendFileDescriptors(fds, buf, offset, length);
    }

    public DomainSocket getDomainSocket() {
      return DomainSocketImpl.this.getDomainSocket0();
    }

    @Override
    public void transferToFully(FileChannel fileCh, long position, int count,
        LongWritable waitForWritableTime, LongWritable transferToTime)
        throws IOException {
      FileDescriptor srcFd = getFd(fileCh);
      int dstFd = fdRef();
      try {
        while (count > 0) {
          int transferred = transferTo0(dstFd, srcFd, position, count);
          if (transferred <= 0) {
            // This shouldn't happen
            throw new IOException("transferTo0 returned negative value: " + transferred);
          }
          count -= transferred;
        }
      } finally {
        fdUnref();
      }
    }
    
    private FileDescriptor getFd(FileChannel fileCh) {
      try {
        return (FileDescriptor)fdField.get(fileCh);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public String toString() {
    return super.toString() + " fd:" + fd;
  }
}
