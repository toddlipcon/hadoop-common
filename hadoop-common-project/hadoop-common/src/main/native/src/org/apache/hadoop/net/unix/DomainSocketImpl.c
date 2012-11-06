/*
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

#define _GNU_SOURCE

#include "exception.h"
#include "org/apache/hadoop/io/nativeio/file_descriptor.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_net_unix_DomainSocketImpl.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <jni.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h> /* for FIONREAD */
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/**
 * Can't pass more than this number of file descriptors in a single message.
 */
#define MAX_PASSED_FDS 16

static jmethodID gSetFd;
static jmethodID gSetBindPathMethodId;
static jmethodID gGetPosition;
static jmethodID gSetPosition;
static jmethodID gGetRemaining;
static jmethodID gGetIntoByteArray;
static jmethodID gSetFromByteArray;
static jfieldID gSocketFieldId;

/**
 * Convert an errno to a socket exception name.
 *
 * Note: we assume that all of these exceptions have a one-argument constructor
 * that takes a string.
 *
 * @return               The exception class name
 */
static const char *errnoToSocketExceptionName(int errnum)
{
  switch (errnum) {
  case ETIMEDOUT:
    return "java/net/SocketTimeoutException";
  case EHOSTDOWN:
  case EHOSTUNREACH:
  case ECONNREFUSED:
    return "java/net/NoRouteToHostException";
  default:
    return "java/io/IOException";
  }
}

static jthrowable newSocketException(JNIEnv *env, int errnum,
                                     const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

static jthrowable newSocketException(JNIEnv *env, int errnum,
                                     const char *fmt, ...)
{
  va_list ap;
  jthrowable jthr;

  va_start(ap, fmt);
  jthr = newExceptionV(env, errnoToSocketExceptionName(errnum), fmt, ap);
  va_end(ap);
  return jthr;
}

static const char* terror(int errnum)
{
  if ((errnum < 0) || (errnum >= sys_nerr)) {
    return "unknown error.";
  }
  return sys_errlist[errnum];
}

/**
 * Flexible buffer that will try to fit data on the stack, and fall back
 * to the heap if necessary.
 */
struct flexibleBuffer {
  int8_t stackBuf[8196];
  int8_t *curBuf;
  int8_t *allocBuf;
};

static jthrowable flexBufInit(JNIEnv *env, struct flexibleBuffer *flexBuf, jint length)
{
  memset(flexBuf, 0, sizeof(*flexBuf));
  if (length < sizeof(flexBuf->stackBuf)) {
    flexBuf->curBuf = flexBuf->stackBuf;
    return NULL;
  }
  flexBuf->allocBuf = calloc(1, length);
  if (!flexBuf->allocBuf) {
    return newRuntimeException(env, "OOM allocating space for %d "
                              "bytes of data.", length);
  }
  flexBuf->curBuf = flexBuf->allocBuf;
  return NULL;
}

static void flexBufFree(struct flexibleBuffer *flexBuf)
{
  free(flexBuf->allocBuf);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_anchorNative(
JNIEnv *env, jclass clazz)
{
  jclass socketImplClazz, bufferClazz, byteBufferClazz;

  if (gSetFromByteArray) {
    return; // already initialized
  }
  fd_init(env); // for fd_get, fd_create, etc.
  if ((*env)->ExceptionCheck(env)) {
    return; // exception was raised
  }
  gSetFd = (*env)->GetMethodID(env, clazz, "setFd", "(I)Z");
  if (!gSetFd) {
    return; // exception was raised
  }
  gSetBindPathMethodId = (*env)->GetMethodID(env, clazz,
          "setBindPath", "(Ljava/lang/String;)V");
  if (!gSetBindPathMethodId) {
    return; // exception was raised
  }
  bufferClazz = (*env)->FindClass(env, "java/nio/Buffer");
  if (!bufferClazz) {
    return; // exception was raised
  }
  gGetPosition = (*env)->GetMethodID(env, bufferClazz,
                                     "position", "()I");
  if (!gGetPosition) {
    return; // exception was raised
  }
  gSetPosition = (*env)->GetMethodID(env, bufferClazz,
                                     "position", "(I)Ljava/nio/Buffer;");
  if (!gSetPosition) {
    return; // exception was raised
  }
  socketImplClazz = (*env)->FindClass(env, "java/net/SocketImpl");
  if (!socketImplClazz) {
    return; // exception was raised
  }
  gSocketFieldId = (*env)->GetFieldID(env, socketImplClazz,
              "socket", "Ljava/net/Socket;");
  if (!gSocketFieldId) {
    return; // exception was raised
  }
  byteBufferClazz = (*env)->FindClass(env, "java/nio/ByteBuffer");
  if (!byteBufferClazz) {
    return; // exception was raised
  }
  gGetRemaining = (*env)->GetMethodID(env, bufferClazz,
                                     "remaining", "()I");
  if (!gGetRemaining) {
    return; // exception was raised
  }
  gGetIntoByteArray = (*env)->GetMethodID(env, byteBufferClazz,
                                     "get", "([B)Ljava/nio/ByteBuffer;");
  if (!gGetRemaining) {
    return; // exception was raised
  }
  gSetFromByteArray = (*env)->GetMethodID(env, byteBufferClazz,
                                     "put", "([B)Ljava/nio/ByteBuffer;");
  if (!gSetFromByteArray) {
    return; // exception was raised
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_getDomainSocket0(
JNIEnv *env, jobject this)
{
  return (*env)->GetObjectField(env, this, gSocketFieldId);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_setTimeout0(
JNIEnv *env, jclass clazz, jint fd, jint timeo,
jboolean setSendTimeout, jboolean setRecvTimeout)
{
  int ret;
  struct timeval tv, stv;

  tv.tv_sec = timeo / 1000;
  tv.tv_usec = (timeo - (tv.tv_sec * 1000)) * 1000;
  if (setRecvTimeout) {
    memcpy(&stv, &tv, sizeof(stv));
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (struct timeval *)&stv,
               sizeof(stv))) {
      ret = errno;
      (*env)->Throw(env, newSocketException(env, ret,
          "setsockopt(SO_RCVTIMEO) error: %s", terror(ret)));
      return;
    }
  }
  if (setSendTimeout) {
    memcpy(&stv, &tv, sizeof(stv));
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (struct timeval *)&stv,
               sizeof(stv))) {
      ret = errno;
      (*env)->Throw(env, newSocketException(env, ret,
          "setsockopt(SO_SNDTIMEO) error: %s", terror(ret)));
      return;
    }
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_getTimeout0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret;
  jint rval;
  struct timeval tv;
  socklen_t len;

  memset(&tv, 0, sizeof(tv));
  len = sizeof(struct timeval);
  if (getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
             &tv, &len)) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
        "getsockopt(SO_RCVTIMEO) error: %s", terror(ret)));
    return -1;
  }
  rval = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
  return rval;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_setBufSize0(
JNIEnv *env, jclass clazz, jint fd, jboolean isSnd, jint bufSize)
{
  int ret, val = bufSize;
  int opt = isSnd ? SO_SNDBUF : SO_RCVBUF;
  const char *optName = isSnd ? "SO_SNDBUF" : "SO_RCVBUF";

  if (setsockopt(fd, SOL_SOCKET, opt, &val, sizeof(val))) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
        "setsockopt(%s) error: %s", optName, terror(ret)));
    return;
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_getBufSize0(
JNIEnv *env, jclass clazz, jint fd, jboolean isSnd)
{
  int val = -1, ret;
  int opt = isSnd ? SO_SNDBUF : SO_RCVBUF;
  const char *optName = isSnd ? "SO_SNDBUF" : "SO_RCVBUF";
  socklen_t len;

  len = sizeof(val);
  if (getsockopt(fd, SOL_SOCKET, opt, &val, &len)) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
        "getsockopt(%s) error: %s", optName, terror(ret)));
    return -1;
  }
#ifdef __linux__
  // Linux always doubles the value that you set with setsockopt.
  // We cut it in half here so that programs can at least read back the same
  // value they set.
  val /= 2;
#endif
  return val;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_create0(
JNIEnv *env, jclass clazz, jboolean stream)
{
  jint fd;
  int ret;

  if (!stream) {
    (*env)->Throw(env, newSocketException(env, ENOTSUP,
        "UNIX domain datagram sockets are not yet supported."));
    return -1;
  }
  fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
        "error creating UNIX domain socket with SOCK_STREAM: %s",
        terror(ret)));
    return -1;
  }
  return fd;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_setup0(
JNIEnv *env, jobject this, jint fd, jobject jpath, jboolean doConnect)
{
  const char *cpath = NULL;
  struct sockaddr_un addr;
  jthrowable jthr = NULL;
  int ret;

  memset(&addr, 0, sizeof(&addr));
  addr.sun_family = AF_UNIX;
  cpath = (*env)->GetStringUTFChars(env, jpath, NULL);
  if (!cpath) {
    jthr = (*env)->ExceptionOccurred(env);
    (*env)->ExceptionClear(env);
    goto done;
  }
  ret = snprintf(addr.sun_path, sizeof(addr.sun_path),
                 "%s", cpath);
  if (ret < 0) {
    jthr = newSocketException(env, EIO,
        "error computing UNIX domain socket path: ");
    goto done;
  }
  if (ret >= sizeof(addr.sun_path)) {
    jthr = newSocketException(env, ENAMETOOLONG,
        "error computing UNIX domain socket path: path too long.");
    goto done;
  }
  if (doConnect) {
    RETRY_ON_EINTR(ret, connect(fd, 
        (struct sockaddr*)&addr, sizeof(addr)));
    if (ret < 0) {
      ret = errno;
      jthr = newException(env, "java/net/ConnectException",
              "connect(2) error: %s when trying to connect to '%s'",
              terror(ret), addr.sun_path);
      goto done;
    }
  } else {
    RETRY_ON_EINTR(ret, unlink(addr.sun_path));
    RETRY_ON_EINTR(ret, bind(fd, (struct sockaddr*)&addr, sizeof(addr)));
    if (ret < 0) {
      ret = errno;
      jthr = newException(env, "java/net/BindException",
                          "bind(2) error: %s", terror(ret));
      goto done;
    }
    // Inject the new path
    (*env)->CallVoidMethod(env, this, gSetBindPathMethodId, jpath);
    jthr = (*env)->ExceptionOccurred(env);
    if (jthr) {
      (*env)->ExceptionClear(env);
      goto done;
    }
  }

done:
  if (cpath) {
    (*env)->ReleaseStringUTFChars(env, jpath, cpath);
  }
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_listen0(
JNIEnv *env, jclass clazz, jint fd, jint backlog)
{
  if (listen(fd, backlog) < 0) {
    int ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
                  "listen(2) error: %s", terror(ret)));
    return;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_accept0(
JNIEnv *env, jclass clazz, jint fd, jobject newSock)
{
  int ret, newFd = -1;
  socklen_t slen;
  struct sockaddr_un remote;
  jthrowable jthr = NULL;

  slen = sizeof(remote);
  do {
    newFd = accept(fd, (struct sockaddr*)&remote, &slen);
  } while ((newFd < 0) && (errno == EINTR));
  if (newFd < 0) {
    ret = errno;
    jthr = newSocketException(env, ret, "accept(2) error: %s", terror(ret));
    goto done;
  }
  // Inject the new file descriptor
  if (JNI_FALSE ==
      (*env)->CallBooleanMethod(env, newSock, gSetFd, newFd)) {
    jthr = newSocketException(env, EINVAL, "The argument to "
        "DomainSocketImpl#accept must be an unused SocketImpl, but "
        "this one is in use.");
    goto done;
  }

done:
  if (jthr) {
    if (newFd > 0) {
      RETRY_ON_EINTR(ret, close(newFd));
    }
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_available0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret, avail = 0;
  jthrowable jthr = NULL;

  RETRY_ON_EINTR(ret, ioctl(fd, FIONREAD, &avail));
  if (ret < 0) {
    ret = errno;
    jthr = newSocketException(env, ret,
              "ioctl(%d, FIONREAD) error: %s", fd, terror(ret));
    goto done;
  }
done:
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
  return avail;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_close0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret;

  if (fd < 0) {
    return; // socket is already closed.
  }
  RETRY_ON_EINTR(ret, close(fd));
  if (ret) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
          "close(2) error: %s", terror(ret)));
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_shutdown0(
JNIEnv *env, jclass clazz, jint fd, jboolean shutdownInput,
jboolean shutdownOutput)
{
  int ret;
  int how = 0;
  const char *howStr = "0";

  if (shutdownInput) {
    if (shutdownOutput) {
      how = SHUT_RDWR;
      howStr = "SHUT_RDWR";
    }
    else {
      how = SHUT_RD;
      howStr = "SHUT_RD";
    }
  } else if (shutdownOutput) {
    how = SHUT_WR;
    howStr = "SHUT_WR";
  }
  RETRY_ON_EINTR(ret, shutdown(fd, how));
  if (ret < 0) {
    ret = errno;
    (*env)->Throw(env, newSocketException(env, ret,
              "shutdown(%d, %s) error: %s", fd, howStr, terror(ret)));
  }
}

/**
 * Write an entire buffer to a file descriptor.
 *
 * @param env            The JNI environment.
 * @param fd             The fd to write to.
 * @param buf            The buffer to write
 * @param amt            The length of the buffer to write.
 * @return               NULL on success; or the unraised exception representing
 *                       the problem.
 */
static jthrowable write_fully(JNIEnv *env, int fd, int8_t *buf, int amt)
{
  int err, res;

  while (amt > 0) {
    res = write(fd, buf, amt);
    if (res < 0) {
      err = errno;
      if (err == EINTR) {
        continue;
      }
      return newSocketException(env, err, "write(2) error: %s", terror(err));
    }
    amt -= res;
    buf += res;
  }
  return NULL;
}

/**
 * Our auxillary data setup.
 *
 * See man 3 cmsg for more information about auxillary socket data on UNIX.
 */
struct cmsghdr_with_fds {
  struct cmsghdr hdr;
  int fds[MAX_PASSED_FDS];
};

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_sendFileDescriptors0(
JNIEnv *env, jclass clazz, jint fd, jobject jfds, jobject jbuf,
jint offset, jint length)
{
  struct iovec vec[1];
  struct flexibleBuffer flexBuf;
  struct cmsghdr_with_fds aux;
  jint jfdsLen;
  int i, ret = -1, auxLen;
  struct msghdr socketMsg;
  jthrowable jthr = NULL;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  if (length <= 0) {
    jthr = newRuntimeException(env, "You must write at least one byte.");
    goto done;
  }
  jfdsLen = (*env)->GetArrayLength(env, jfds);
  if (jfdsLen <= 0) {
    jthr = newRuntimeException(env, "Called sendFileDescriptors with "
                               "no file descriptors.");
    goto done;
  } else if (jfdsLen > MAX_PASSED_FDS) {
    jfdsLen = 0;
    jthr = newRuntimeException(env, "Called sendFileDescriptors with "
          "an array of %d length.  The maximum is %d.", jfdsLen,
          MAX_PASSED_FDS);
    goto done;
  }
  (*env)->GetByteArrayRegion(env, jbuf, offset, length, flexBuf.curBuf); 
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
  memset(&vec, 0, sizeof(vec));
  vec[0].iov_base = flexBuf.curBuf;
  vec[0].iov_len = length;
  auxLen = CMSG_LEN(jfdsLen * sizeof(int));
  memset(&aux, 0, auxLen);
  memset(&socketMsg, 0, sizeof(socketMsg));
  socketMsg.msg_iov = vec;
  socketMsg.msg_iovlen = 1;
  socketMsg.msg_control = &aux;
  socketMsg.msg_controllen = auxLen;
  aux.hdr.cmsg_len = auxLen;
  aux.hdr.cmsg_level = SOL_SOCKET;
  aux.hdr.cmsg_type = SCM_RIGHTS;
  for (i = 0; i < jfdsLen; i++) {
    jobject jfd = (*env)->GetObjectArrayElement(env, jfds, i);
    if (!jfd) {
      jthr = (*env)->ExceptionOccurred(env);
      if (jthr) {
        (*env)->ExceptionClear(env);
        goto done;
      }
      jthr = newRuntimeException(env, "element %d of jfds was NULL.", i);
      goto done;
    }
    aux.fds[i] = fd_get(env, jfd);
    (*env)->DeleteLocalRef(env, jfd);
    if (jthr) {
      goto done;
    }
  }
  RETRY_ON_EINTR(ret, sendmsg(fd, &socketMsg, 0));
  if (ret < 0) {
    ret = errno;
    jthr = newSocketException(env, ret, "sendmsg(2) error: %s", terror(ret));
    goto done;
  }
  length -= ret;
  if (length > 0) {
    // Write the rest of the bytes we were asked to send.
    // This time, no fds will be attached.
    jthr = write_fully(env, fd, flexBuf.curBuf + ret, length);
    if (jthr) {
      goto done;
    }
  }

done:
  flexBufFree(&flexBuf);
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_recvFileDescriptors0(
JNIEnv *env, jclass clazz, jint fd, jarray jfds, jarray jbuf,
jint offset, jint length)
{
  struct iovec vec[1];
  struct flexibleBuffer flexBuf;
  struct cmsghdr_with_fds aux;
  int i, jRecvFdsLen = 0, auxLen;
  jint jfdsLen = 0;
  struct msghdr socketMsg;
  ssize_t bytesRead = -1;
  jobject fdObj;
  jthrowable jthr = NULL;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  if (length <= 0) {
    jthr = newRuntimeException(env, "You must read at least one byte.");
    goto done;
  }
  jfdsLen = (*env)->GetArrayLength(env, jfds);
  if (jfdsLen <= 0) {
    jthr = newRuntimeException(env, "Called recvFileDescriptors with "
          "an array of %d length.  You must pass at least one fd.", jfdsLen);
    goto done;
  } else if (jfdsLen > MAX_PASSED_FDS) {
    jfdsLen = 0;
    jthr = newRuntimeException(env, "Called recvFileDescriptors with "
          "an array of %d length.  The maximum is %d.", jfdsLen,
          MAX_PASSED_FDS);
    goto done;
  }
  for (i = 0; i < jfdsLen; i++) {
    (*env)->SetObjectArrayElement(env, jfds, i, NULL);
  }
  vec[0].iov_base = flexBuf.curBuf;
  vec[0].iov_len = length;
  auxLen = CMSG_LEN(jfdsLen * sizeof(int));
  memset(&aux, 0, auxLen);
  memset(&socketMsg, 0, auxLen);
  socketMsg.msg_iov = vec;
  socketMsg.msg_iovlen = 1;
  socketMsg.msg_control = &aux;
  socketMsg.msg_controllen = auxLen;
  aux.hdr.cmsg_len = auxLen;
  aux.hdr.cmsg_level = SOL_SOCKET;
  aux.hdr.cmsg_type = SCM_RIGHTS;
  RETRY_ON_EINTR(bytesRead, recvmsg(fd, &socketMsg, 0));
  if (bytesRead < 0) {
    int ret = errno;
    if (ret == ECONNABORTED) {
      // The remote peer disconnected on us.  Treat this as an EOF.
      bytesRead = -1;
      goto done;
    }
    jthr = newSocketException(env, ret, "recvmsg(2) failed: %s",
                              terror(ret));
    goto done;
  } else if (bytesRead == 0) {
    jthr = newException(env, "java/io/EOFException", "unexpected EOF "
                        "when trying to read file descriptors.");
    goto done;
  }
//  if ((socketMsg.msg_flags & MSG_CTRUNC) == MSG_CTRUNC) {
//    jthr = newSocketException(env, EINVAL,
//        "recvFileDescriptors failed: server "
//        "gave more ancillary data than we expected: ");
//    goto done;
//  }
//  if (controlMsg->cmsg_type != SCM_RIGHTS) {
//    return newSocketException(env, EIO, "recvFileDescriptors "
//        "failed: got control message of unknown type: ");
//  }
  jRecvFdsLen = (aux.hdr.cmsg_len - sizeof(struct cmsghdr)) / sizeof(int);
  for (i = 0; i < jRecvFdsLen; i++) {
    fdObj = fd_create(env, aux.fds[i]);
    if (!fdObj) {
      jthr = (*env)->ExceptionOccurred(env);
      (*env)->ExceptionClear(env);
      goto done;
    }
    // Make this -1 so we don't attempt to close it twice in an error path.
    aux.fds[i] = -1;
    (*env)->SetObjectArrayElement(env, jfds, i, fdObj);
    // There is no point keeping around a local reference to the fdObj.
    // The array continues to reference it.
    (*env)->DeleteLocalRef(env, fdObj);
  }
  (*env)->SetByteArrayRegion(env, jbuf, offset, length, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
done:
  flexBufFree(&flexBuf);
  if (jthr) {
    // Free any FileDescriptor references we may have created,
    // or file descriptors we may have been passed.
    for (i = 0; i < jRecvFdsLen; i++) {
      if (aux.fds[i] >= 0) {
        RETRY_ON_EINTR(i, close(aux.fds[i]));
        aux.fds[i] = -1;
      }
      fdObj = (*env)->GetObjectArrayElement(env, jfds, i);
      if (fdObj) {
        int ret, afd = fd_get(env, fdObj);
        if (afd >= 0) {
          RETRY_ON_EINTR(ret, close(afd));
        }
        (*env)->SetObjectArrayElement(env, jfds, i, NULL);
        (*env)->DeleteLocalRef(env, fdObj);
      }
    }
    (*env)->Throw(env, jthr);
  }
  return bytesRead;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_closeFileDescriptor0(
JNIEnv *env, jclass clazz, jobject jfd)
{
  int ret, rfd;

  rfd = fd_get(env, jfd);
  if (rfd >= 0) {
    RETRY_ON_EINTR(ret, close(rfd));
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_read0(
JNIEnv *env, jclass clazz, jint fd)
{
  int ret;
  int8_t buf[1] = { 0 };

  RETRY_ON_EINTR(ret, read(fd, buf, sizeof(buf)));
  if (ret < 0) {
    ret = errno;
    if (ret == ECONNABORTED) {
      // The remote peer disconnected on us.  Treat this as an EOF.
      return -1;
    }
    (*env)->Throw(env,
      newSocketException(env, ret, "read(2) error: %s", terror(ret)));
    return 0;
  }
  if (ret == 0) {
    // Java wants -1 on EOF
    return -1;
  }
  return buf[0];
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_readArray0(
JNIEnv *env, jclass clazz, jint fd, jarray b, jint offset, jint length)
{
  int ret = -1;
  struct flexibleBuffer flexBuf;
  jthrowable jthr;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  RETRY_ON_EINTR(ret, read(fd, flexBuf.curBuf, length));
  if (ret < 0) {
    ret = errno;
    if (ret == ECONNABORTED) {
      // The remote peer disconnected on us.  Treat this as an EOF.
      ret = -1;
      goto done;
    }
    jthr = newSocketException(env, ret, "read(2) error: %s", 
                              terror(ret));
    goto done;
  }
  if (ret == 0) {
    goto done;
  }
  (*env)->SetByteArrayRegion(env, b, offset, ret, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
done:
  flexBufFree(&flexBuf);
  if (jthr) { 
    (*env)->Throw(env, jthr);
  }
  return ret == 0 ? -1 : ret; // Java wants -1 on EOF
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_write0(
JNIEnv *env, jclass clazz, jint fd, jint b)
{
  int8_t buf[1] = { b };
  jthrowable jthr;

  jthr = write_fully(env, fd, buf, sizeof(buf));
  if (jthr) {
    (*env)->Throw(env, jthr);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_writeArray0(
JNIEnv *env, jclass clazz, jint fd, jarray b, jint offset, jint length)
{
  struct flexibleBuffer flexBuf;
  jthrowable jthr;

  jthr = flexBufInit(env, &flexBuf, length);
  if (jthr) {
    goto done;
  }
  (*env)->GetByteArrayRegion(env, b, offset, length, flexBuf.curBuf);
  jthr = (*env)->ExceptionOccurred(env);
  if (jthr) {
    (*env)->ExceptionClear(env);
    goto done;
  }
  jthr = write_fully(env, fd, flexBuf.curBuf, length);
  if (jthr) {
    goto done;
  }

done:
  flexBufFree(&flexBuf);
  if (jthr) { 
    (*env)->Throw(env, jthr);
  }
}

struct byteBufferAdaptor {
  jobject bb;
  int8_t *allocBuf;
  int8_t *curBuf;
  int curBufLen;
  jint origPosition;
};

static jthrowable byteBufferAdaptorInit(JNIEnv *env,
    struct byteBufferAdaptor *adapt, jobject bb, int isWrite)
{
  int8_t *ptr = NULL;
  jint remaining;
  jarray jarr = NULL;
  jthrowable jthr;

  memset(adapt, 0, sizeof(*adapt));
  adapt->bb = bb;
  adapt->origPosition = (*env)->CallIntMethod(env, bb, gGetPosition);
  adapt->curBufLen = remaining = (*env)->CallIntMethod(env, bb, gGetRemaining);
  ptr = (*env)->GetDirectBufferAddress(env, bb);
  if (ptr) {
    adapt->curBuf = ptr + adapt->origPosition;
    return NULL;
  }
  ptr = calloc(1, remaining);
  if (!ptr) {
    memset(adapt, 0, sizeof(*adapt));
    return newRuntimeException(env, "OOM allocating space for %d "
                              "bytes of data.", remaining);
  }
  if (isWrite) {
    jarr = (*env)->NewByteArray(env, remaining);
    if (!jarr) {
      goto handle_exception;
    }
    (*env)->CallVoidMethod(env, bb, gGetIntoByteArray, jarr);
    if ((*env)->ExceptionCheck(env)) {
      goto handle_exception;
    }
    (*env)->GetByteArrayRegion(env, jarr, 0, remaining, ptr);
    if ((*env)->ExceptionCheck(env)) {
      goto handle_exception;
    }
  }
  adapt->curBuf = adapt->allocBuf = ptr;
  (*env)->DeleteLocalRef(env, jarr);
  return NULL;

handle_exception:
  jthr = (*env)->ExceptionOccurred(env);
  (*env)->ExceptionClear(env);
  (*env)->DeleteLocalRef(env, jarr);
  free(ptr);
  memset(adapt, 0, sizeof(*adapt));
  return jthr;
}

/**
 * Frees the byteBufferAdaptor.
 *
 * If the bye buffer is not direct, and the operation is a read operation,
 * copies the bytes from the byteBufferAdaptor back into the byte buffer.
 *
 * @param env                The JNI environment.
 * @param adapt              The byteBufferAdaptor.
 * @param used               How much of the ByteBuffer was written or read.
 * @param isWrite            True if the operation is a write operation
 *
 * @return                   The exception, or NULL if there is none.
 */
static jthrowable byteBufferAdaptorClose(JNIEnv *env,
          struct byteBufferAdaptor *adapt, jint used, int isWrite)
{
  jarray jarr = NULL;
  jthrowable jthr;
  jint newPosition;

  if ((!adapt->allocBuf) || isWrite) {
    // If we are using a direct byte buffer, or we are writing rather than
    // reading, all that remains to do is update ByteBuffer#position.
    newPosition = adapt->origPosition + used;
    (*env)->CallIntMethod(env, adapt->bb, gSetPosition, newPosition);
  } else {
    // Handle reading into an indirect byte buffer.
    // In this case, we need to copy back what we wrote into the
    // byteBufferAdaptor's local buffer into the actual ByteBuffer.
    // Assumptions: the ByteBuffer's position should be the same as it was in
    // byteBufferAdaptorInit, since we never did anything that would change the
    // position.
    jarr = (*env)->NewByteArray(env, used);
    if (!jarr) {
      goto handle_exception;
    }
    (*env)->SetByteArrayRegion(env, jarr, 0, used, adapt->curBuf);
    if ((*env)->ExceptionCheck(env)) {
      goto handle_exception;
    }
    (*env)->CallVoidMethod(env, adapt->bb, gSetFromByteArray, jarr);
    if ((*env)->ExceptionCheck(env)) {
      goto handle_exception;
    }
  }
  (*env)->DeleteLocalRef(env, jarr);
  return NULL;

handle_exception:
  jthr = (*env)->ExceptionOccurred(env);
  (*env)->ExceptionClear(env);
  (*env)->DeleteLocalRef(env, jarr);
  return jthr;
}

static void byteBufferAdaptorFree(struct byteBufferAdaptor *adapt)
{
  free(adapt->allocBuf);
  memset(adapt, 0, sizeof(*adapt));
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_readByteBuffer0(
JNIEnv *env, jclass clazz, jint fd, jobject bb)
{
  jthrowable jthr;
  struct byteBufferAdaptor adapt;
  int res;

  jthr = byteBufferAdaptorInit(env, &adapt, bb, 0);
  if (jthr) {
    goto error;
  }
  RETRY_ON_EINTR(res, read(fd, adapt.curBuf, adapt.curBufLen));
  if (res < 0) {
    res = errno;
    if (res != ECONNABORTED) {
      jthr = newSocketException(env, res, "read(2) error: %s", 
                                terror(res));
      goto error;
    } else {
      // The remote peer disconnected on us.  Treat this as an EOF.
      res = -1;
    }
  }
  jthr = byteBufferAdaptorClose(env, &adapt, res, 0);
  if (jthr) {
    goto error;
  }
  byteBufferAdaptorFree(&adapt);
  return res;

error:
  byteBufferAdaptorFree(&adapt);
  (*env)->Throw(env, jthr);
  return -1;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_net_unix_DomainSocketImpl_writeByteBuffer0(
JNIEnv *env, jclass clazz, jint fd, jobject bb)
{
  jthrowable jthr;
  struct byteBufferAdaptor adapt;
  jint total;

  /* The API documentation for WritableByteChannel#write is ambiguous about
   * whether or not we should write as much as possible.  Reading between the
   * lines, short writes seem to be returned only in the case of non-blocking
   * sockets, which we don't support here.  So we just do the simple thing and
   * try to write it all.
   */
  jthr = byteBufferAdaptorInit(env, &adapt, bb, 1);
  if (jthr) {
    goto error;
  }
  total = adapt.curBufLen;
  jthr = write_fully(env, fd, adapt.curBuf, total);
  if (jthr) {
    goto error;
  }
  jthr = byteBufferAdaptorClose(env, &adapt, total, 1);
  if (jthr) {
    goto error;
  }
  byteBufferAdaptorFree(&adapt);
  return total;

error:
  byteBufferAdaptorFree(&adapt);
  (*env)->Throw(env, jthr);
  return -1;
}
