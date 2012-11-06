package org.apache.hadoop.net.unix;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class DomainSocketChannel extends SocketChannel {
  final private Socket socket;
  final private DomainSocketImpl dsImpl;
  
  public DomainSocketChannel(SelectorProvider provider,
      Socket socket, DomainSocketImpl impl) {
    super(provider);
    this.socket = socket;
    this.dsImpl = impl;
  }

  @Override
  public Socket socket() {
    return socket;
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public boolean isConnectionPending() {
    return false;
  }

  @Override
  public boolean connect(SocketAddress remote) throws IOException {
    throw new AlreadyConnectedException();
  }

  @Override
  public boolean finishConnect() throws IOException {
    return false;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return dsImpl.readByteBuffer(dst);
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length)
      throws IOException {
    return dsImpl.readByteBufferArray(dsts, offset, length);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return dsImpl.writeByteBuffer(src);
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
      throws IOException {
    return dsImpl.writeByteBufferArray(srcs, offset, length);
  }

  @Override
  protected void implCloseSelectableChannel() throws IOException {
    socket.close();
  }

  @Override
  protected void implConfigureBlocking(boolean block) throws IOException {
    if (!block) {
      throw new UnsupportedOperationException("DomainSocketChannel does " +
          "not support non-blocking operation.");
    }
  }
}
