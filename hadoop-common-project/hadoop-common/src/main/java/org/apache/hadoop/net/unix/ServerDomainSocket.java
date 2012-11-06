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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketImpl;

import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.annotations.VisibleForTesting;

/**
 * A ServerSocket for UNIX domain sockets.
 */
public class ServerDomainSocket extends ServerSocket {
  static native void anchorNative();

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      anchorNative();
    }
  }

  public ServerDomainSocket() throws IOException {
    super();
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      throw new UnsupportedOperationException("Can't create " +
          "DomainSocketImpl: libhadoop.so has not been loaded.");
    }
    setSocketImpl(new DomainSocketImpl());
  }

  public void setPath(String path) {
    getDomainSocketImpl().setSetupPath(path);
  }

  @Override
  public Socket accept() throws IOException {
    DomainSocket newSock = new DomainSocket();
    implAccept(newSock);
    return newSock;
  }

  native void setSocketImpl(DomainSocketImpl impl);

  @VisibleForTesting
  native SocketImpl getSocketImpl();

  @VisibleForTesting
  DomainSocketImpl getDomainSocketImpl() {
    SocketImpl sockImpl = getSocketImpl();
    return (DomainSocketImpl)sockImpl;
  }
  
  /**
   * Get the bind path.
   *
   * @return           The bind path, or nulL if the socket hasn't been bound.
   */
  public String getBindPath() {
    return getDomainSocketImpl().getBindPath();
  }
}