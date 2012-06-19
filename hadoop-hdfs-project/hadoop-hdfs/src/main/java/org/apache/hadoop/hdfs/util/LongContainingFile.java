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
package org.apache.hadoop.hdfs.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

@InterfaceAudience.Private
public abstract class LongContainingFile {

  private static final Log LOG = LogFactory.getLog(
      LongContainingFile.class);

  public static void write(File file, long val) throws IOException {
    OutputStream fos = new AtomicFileOutputStream(file);
    try {
      fos.write(String.valueOf(val).getBytes());
      fos.write('\n');
      fos.close();
      fos = null;
    } finally {
      // TODO use abort() here if unsuccessful!
      IOUtils.cleanup(LOG, fos);
    }
  }

  public static long read(File file, long defaultVal) throws IOException {
    long txid = defaultVal;
    // TODO: canRead below seems wrong
    if (file.exists() && file.canRead()) {
      BufferedReader br = new BufferedReader(new FileReader(file));
      try {
        txid = Long.valueOf(br.readLine());
        br.close();
        br = null;
      } finally {
        IOUtils.cleanup(LOG, br);
      }
    }
    return txid;
  }
}
