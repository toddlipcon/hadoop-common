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

#include <arpa/inet.h>
#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "config.h"
#include "org_apache_hadoop.h"
#include "org_apache_hadoop_util_NativeCrc32.h"
#include "gcc_optimizations.h"
#include "bulk_crc32.h"

static jclass byteBuffer_clazz;
static jmethodID byteBuffer_arrayMethod;
static jmethodID byteBuffer_arrayOffsetMethod;

static void throw_checksum_exception(JNIEnv *env,
    uint32_t got_crc, uint32_t expected_crc,
    jstring j_filename, jlong pos) {
  char message[1024];
  jstring jstr_message;
  char *filename;

  // Get filename as C string, or "null" if not provided
  if (j_filename == NULL) {
    filename = strdup("null");
  } else {
    const char *c_filename = (*env)->GetStringUTFChars(env, j_filename, NULL);
    if (c_filename == NULL) {
      return; // OOME already thrown
    }
    filename = strdup(c_filename);
    (*env)->ReleaseStringUTFChars(env, j_filename, c_filename);
  }

  // Format error message
  snprintf(message, sizeof(message),
    "Checksum error: %s at %"PRId64" exp: %"PRId32" got: %"PRId32,
    filename, pos, expected_crc, got_crc);
  if ((jstr_message = (*env)->NewStringUTF(env, message)) == NULL) {
    goto cleanup;
  }
 
  // Throw exception
  jclass checksum_exception_clazz = (*env)->FindClass(
    env, "org/apache/hadoop/fs/ChecksumException");
  if (checksum_exception_clazz == NULL) {
    goto cleanup;
  }

  jmethodID checksum_exception_ctor = (*env)->GetMethodID(env,
    checksum_exception_clazz, "<init>",
    "(Ljava/lang/String;J)V");
  if (checksum_exception_ctor == NULL) {
    goto cleanup;
  }

  jthrowable obj = (jthrowable)(*env)->NewObject(env, checksum_exception_clazz,
    checksum_exception_ctor, jstr_message, pos);
  if (obj == NULL) goto cleanup;

  (*env)->Throw(env, obj);

cleanup:
  if (filename != NULL) {
    free(filename);
  }
}

static int convert_java_crc_type(JNIEnv *env, jint crc_type) {
  switch (crc_type) {
    case org_apache_hadoop_util_NativeCrc32_CHECKSUM_CRC32:
      return CRC32_ZLIB_POLYNOMIAL;
    case org_apache_hadoop_util_NativeCrc32_CHECKSUM_CRC32C:
      return CRC32C_POLYNOMIAL;
    default:
      THROW(env, "java/lang/IllegalArgumentException",
        "Invalid checksum type");
      return -1;
  }
}

/**
 * Verify parameters for NULL, valid ranges.
 * Returns non-zero and throws an exception if invalid.
 */
static int check_params(JNIEnv *env,
    jobject j_sums, jint sums_offset,
    jobject j_data, jint data_offset, jint data_len,
    jint bytes_per_checksum, jint j_crc_type,
    int *crc_type)
{
  if (unlikely(!j_sums || !j_data)) {
    THROW(env, "java/lang/NullPointerException",
      "input ByteBuffers must not be null");
    return 1;
  }

  if (unlikely(sums_offset < 0 || data_offset < 0 || data_len < 0)) {
    THROW(env, "java/lang/IllegalArgumentException",
      "bad offsets or lengths");
    return 1;
  }
  if (unlikely(bytes_per_checksum) <= 0) {
    THROW(env, "java/lang/IllegalArgumentException",
      "invalid bytes_per_checksum");
    return 1;
  }

  // Convert to correct internal C constant for CRC type
  *crc_type = convert_java_crc_type(env, j_crc_type);
  if (*crc_type == -1) {
    return 1; // exception thrown
  }

  return 0;
}

typedef struct bbuf {
  // Only valid for direct buffers:
  uint8_t *addr;

  // Only valid for non-direct buffers:
  jarray array;
  jint array_offset;
} bbuf_t;


static int bytebuffer_resolve(JNIEnv *env, jobject bytebuf, bbuf_t *resolved) {
  // First, clear fields
  resolved->array = NULL;
  resolved->array_offset = 0;
  resolved->addr = NULL;

  // Assume it's a direct buf
  resolved->addr = (*env)->GetDirectBufferAddress(env, bytebuf);
  if (likely(resolved->addr != NULL)) {
    // success
    return 0;
  }

  // Not a direct buf, grab the array inside.
  resolved->array = (jarray) (*env)->CallObjectMethod(env,
      bytebuf, byteBuffer_arrayMethod);
  if (!resolved->array) {
    return 1; // exception already thrown
  }

  resolved->array_offset = (*env)->CallIntMethod(env,
      bytebuf, byteBuffer_arrayOffsetMethod);
  PASS_EXCEPTIONS_RET(env, 1);

  return 0;
}


static uint8_t *bytebuffer_get(JNIEnv *env, const bbuf_t *bbuf) {
  if (bbuf->addr) {
    return bbuf->addr;
  }

  uint8_t *ret = (*env)->GetPrimitiveArrayCritical(env, bbuf->array, 0);
  if (likely(ret != NULL)) {
    return ret + bbuf->array_offset;
  } else {
    return ret; // NULL
  }
}

static void bytebuffer_release(JNIEnv *env, const bbuf_t *bbuf, uint8_t *addr, jint mode) {
  if (addr == NULL) return;

  if (bbuf->array) {
  // Subtract back the array_offset, since we added it in the return of the
  // get() method
    (*env)->ReleasePrimitiveArrayCritical(env, bbuf->array,
      addr - bbuf->array_offset, mode);
  }
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_util_NativeCrc32_nativeVerifyChunkedSums
  (JNIEnv *env, jclass clazz,
    jint bytes_per_checksum, jint j_crc_type,
    jobject j_sums, jint sums_offset,
    jobject j_data, jint data_offset, jint data_len,
    jstring j_filename, jlong base_pos)
{
  int crc_type;
  if (unlikely(check_params(env,
        j_sums, sums_offset,
        j_data, data_offset, data_len,
        bytes_per_checksum,
        j_crc_type, &crc_type))) {
    return; // exception thrown
  }

  // Convert direct byte buffers to C pointers
  bbuf_t data_bbuf, sums_bbuf;
  if (bytebuffer_resolve(env, j_data, &data_bbuf)) {
    return; // exception thrown
  }

  if (bytebuffer_resolve(env, j_sums, &sums_bbuf)) {
    return; // exception thrown
  }

  // If we end up going to cleanup before even verifying
  // any sums, it's due to OOME (ie we can't grab a pointer to the
  // bytebuffer array, so it returns null)
  uint8_t *data_addr = NULL;
  uint8_t *sums_addr = NULL;

  if ((data_addr = bytebuffer_get(env, &data_bbuf)) == NULL) {
    THROW(env, "java/lang/OutOfMemoryError", "unable to get array data");
    return;
  }

  if ((sums_addr = bytebuffer_get(env, &sums_bbuf)) == NULL) {
    // Have to release the data buffer, or else it's illegal to
    // throw an exception.
    bytebuffer_release(env, &data_bbuf, data_addr, JNI_ABORT);
    THROW(env, "java/lang/OutOfMemoryError", "unable to get array data");
    return;
  }

  uint32_t *sums = (uint32_t *)(sums_addr + sums_offset);
  uint8_t *data = data_addr + data_offset;

  // Setup complete. Actually verify checksums.
  crc32_error_t error_data;
  int ret = bulk_verify_crc(data, data_len, sums, crc_type,
                            bytes_per_checksum, &error_data);

  // The JNI_ABORT mode is used because we haven't modified
  // the data, and hence do not need to memcpy it back
  bytebuffer_release(env, &data_bbuf, data_addr, JNI_ABORT);
  bytebuffer_release(env, &sums_bbuf, sums_addr, JNI_ABORT);

  if (likely(ret == CHECKSUMS_VALID)) {
    return;
  } else if (unlikely(ret == INVALID_CHECKSUM_DETECTED)) {
    long pos = base_pos + (error_data.bad_data - data);
    throw_checksum_exception(
      env, error_data.got_crc, error_data.expected_crc,
      j_filename, pos);
  } else {
    THROW(env, "java/lang/AssertionError",
      "Bad response code from native bulk_verify_crc");
  }
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_util_NativeCrc32_initNative
  (JNIEnv *env, jclass clazz)
{
  byteBuffer_clazz = (*env)->FindClass(env, "java/nio/ByteBuffer");
  PASS_EXCEPTIONS(env);
  byteBuffer_clazz = (*env)->NewGlobalRef(env, byteBuffer_clazz);

  byteBuffer_arrayMethod = (*env)->GetMethodID(
      env, byteBuffer_clazz , "array", "()[B");
  PASS_EXCEPTIONS_GOTO(env, error);

  byteBuffer_arrayOffsetMethod = (*env)->GetMethodID(
      env, byteBuffer_clazz , "arrayOffset", "()I");
  PASS_EXCEPTIONS_GOTO(env, error);

  return;

error:
  (*env)->DeleteGlobalRef(env, byteBuffer_clazz);
}
/**
 * vim: sw=2: ts=2: et:
 */
