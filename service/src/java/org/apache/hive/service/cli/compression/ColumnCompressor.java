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

package org.apache.hive.service.cli.compression;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hive.service.cli.Column;


/**
 * An interface that can be implemented to implement a ResultSet Compressor.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ColumnCompressor {

  /**
   * Before calling the compress() method, should check if the column is compressible by any
   * given plugin.
   *
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  boolean isCompressible(Column col);

  /**
   * Compresses a given column.
   *
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  byte[] compress(Column col);

  /**
   * Used to register a vendor name of the plugin with Service Loader.
   * @return vendor name
   */
  String getVendor();

  /**
   * Used to register a compressorSet name of the plugin with Service Loader.
   * @return compressorSet as a string
   */
  String getCompressorSet();
}
