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

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImpl;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.TreeSet;

import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wraps a UNIX domain socket.
 *
 * A UNIX domain socket is a special type of socket.  Unlike TCP sockets
 * and UDP sockets, it can only be used for communication between processes
 * local to the same computer.  You can pass file descriptors over UNIX domain
 * sockets.
 *
 * Unlike TCP sockets, which are uniquely identified by the 4-tuple of
 * (local address, local port, remote address, remote port), UNIX domain
 * sockets are uniquely identified by just one thing: the local socket inode.
 * However, in order to make interoperating with code that uses TCP sockets
 * easier, the DomainSocket class allows you to set a port number.  If there is
 * a '%d' sequence in the setupPath, it will be replaced with this port number
 * when you call connect, bind, or accept.
 * UNIX domain sockets will always appear to be bound to localhost.
 *
 * For more information about UNIX domain sockets, consult "man 8 unix"
 */
public class DomainSocket extends Socket {
  static native void anchorNative();
  
  private SocketChannel socketChannel = null;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      anchorNative();
    }
  }

  DomainSocket(DomainSocketImpl impl) throws SocketException {
    super(impl);
  }

  public DomainSocket() throws SocketException {
    this(new DomainSocketImpl());
  }

  /**
   * Send some FileDescriptor objects to the process on the other side of this
   * socket.
   *
   * @param fds               The FileDescriptors to send.  These will be
   *                          treated as if they were duplicated with dup(2).
   *                          This means that if the remote process changes the
   *                          file descriptors' offset, this change will be
   *                          visible to you.  It is safe to close these file
   *                          descriptors immediately after passing them.
   * @param buf               The bytes to send.  We will try to send as
   *                          much as possible.
   * @param offset            Offset into buf to use.
   * @param length            Number of bytes from buf to use
   */
  public void sendFileDescriptors(FileDescriptor fds[], byte buf[],
      int offset, int length) throws IOException {
    getDomainSocketImpl().
      sendFileDescriptors(fds, buf, offset, length);
  }
  
  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket, and wrap them in FileInputStream objects.
   *
   * @param fis               (output parameter) Array of FileInputStreams.
   *                          We will fill as many slots as possible with 
   *                          FileInputStreams created with file descriptors 
   *                          passed from the remote process.  The other slots
   *                          will contain NULL.
   * @param buf               (output parameter) Buffer to read into.
   *                          The UNIX domain sockets API requires you to read 
   *                          at least one byte from the remote process, even if
   *                          all you care about is the file descriptors you will
   *                          receive.
   * @param offset            Offset into the byte buffer to load data
   * @param length            Length of the byte buffer to use for data
   *
   * @return                  The number of bytes read, or -1 on EOF.
   *                          We will never return 0.
   */
  public int recvFileInputStreams(FileInputStream[] fis, byte buf[],
        int offset, int length) throws IOException {
    return getDomainSocketImpl().
        recvFileInputStreams(fis, buf, offset, length);
  }

  /**
   * Set the path to use when connecting this DomainSocket to a
   * ServerDomainSocket.
   *
   * If the escape sequence '%d' appears in the configured socket path,
   * we will replace this with the port number.  To get a literal '%d',
   * use '%%d'.
   *
   * @param path              The path to use.
   */
  public void setPath(String path) {
    getDomainSocketImpl().setSetupPath(path);
  }

  @VisibleForTesting
  native SocketImpl getSocketImpl();
  
  @VisibleForTesting
  DomainSocketImpl getDomainSocketImpl() {
    SocketImpl sockImpl = getSocketImpl();
    return (DomainSocketImpl)sockImpl;
  }

  @Override
  public SocketChannel getChannel() {
    DomainSocketImpl dsImpl = getDomainSocketImpl();
    synchronized (dsImpl) {
      if (socketChannel == null) {
        socketChannel = new DomainSocketChannel(SelectorProvider.provider(),
                                                this, dsImpl);
      }
      return socketChannel;
    }
  }

  public void setSendTimeout(int timeo) throws IOException {
    getDomainSocketImpl().setTimeouts(timeo, true, false);
  }
  
  public static class Cache {
    private TreeSet<String> set = new TreeSet<String>();

    public boolean contains(String path, int port) {
      String epath = DomainSocketImpl.getEffectivePath(path, port);
      return set.contains(epath);
    }
    
    public void add(String path, int port) {
      String epath = DomainSocketImpl.getEffectivePath(path, port);
      set.add(epath);
    }
  }
}
