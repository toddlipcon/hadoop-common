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
package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import java.io.FileDescriptor;
import java.io.IOException;

/**
 * Interface that a stream may implement to signify that it can
 * potentially expose a FileDescriptor object corresponding to the
 * main data stream.
 *
 * This interface is primarily used for the ability to fadvise
 * streams on the local file system from within MapReduce.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface HasFd {
  /**
   * @return the file descriptor associated with this stream.
   * If the stream has a separate checksum stream, this should return
   * the file descriptor associated with the main data. This function
   * may return null if no FileDescriptor is associated with the
   * stream.
   */
  public FileDescriptor getFD() throws IOException;
}
