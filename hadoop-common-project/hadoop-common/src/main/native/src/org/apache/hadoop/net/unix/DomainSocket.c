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

#include "org_apache_hadoop_net_unix_DomainSocket.h"

#include <jni.h>

static jfieldID gImplField;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_anchorNative(
JNIEnv *env, jclass clazz)
{
  gImplField = (*env)->GetFieldID(env, clazz, "impl",
        "Ljava/net/SocketImpl;");
  if (!gImplField) {
    return; // exception was raised
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_net_unix_DomainSocket_getSocketImpl(
JNIEnv *env, jobject this)
{
  return (*env)->GetObjectField(env, this, gImplField);
}
