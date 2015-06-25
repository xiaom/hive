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

import java.io.IOException;

import org.apache.hive.service.cli.Column;
import org.xerial.snappy.Snappy;

/**
 * A ColumnCompressor implementation that uses snappy for compression.
 *
 */
public class SnappyIntCompressor implements ColumnCompressor {

  /**
   * Before calling the compress() method below, should check if the column is compressible by any
   * given plugin.
   *
   * @Param col Column to be checked
   * @return boolean which says yes/no
   */
  @Override
  public boolean isCompressible(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Method provided for testing purposes. Decompresses a compressed byte array. should be
   * compressed using the compress() function below which uses the snappy JAR files. If Snappy JAR
   * file isn't used, this method will return the wrong result.
   */
  public static int[] decompress(byte[] input) {
    try {
      return Snappy.uncompressIntArray(input);
    } catch (IOException e) {
      return new int[0];
    }
  }

  /**
   * Compresses a given column.
   * 
   * @Param col A column to be compressed
   * @return byte array with compressed data
   */
  @Override
  public byte[] compress(Column col) {
    switch (col.getType()) {
      case INT_TYPE:
        return Snappy.compress(col.getInts());
      default:
        return new byte[0];
    }
  }

  @Override
  public String getCompressorSet() {
    return "snappy";
  }

  @Override
  public String getVendor() {
    return "snappy";
  }
}
