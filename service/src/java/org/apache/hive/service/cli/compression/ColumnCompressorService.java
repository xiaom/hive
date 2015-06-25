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

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Load classes that implement ColumnCompressor when starting up, and serve
 * them at run time.
 *
 */
public class ColumnCompressorService {
  private static ColumnCompressorService service;
  private ServiceLoader<ColumnCompressor> loader;
  private ConcurrentHashMap<String, Class<? extends ColumnCompressor>> compressorTable
  = new ConcurrentHashMap<String, Class<? extends ColumnCompressor>>();

  private ColumnCompressorService() {
    loader = ServiceLoader.load(ColumnCompressor.class);
  }

  /**
   * Get the singleton instance of ColumnCompressorService.
   *
   * @return A singleton instance of ColumnCompressorService.
   */
  public static synchronized ColumnCompressorService getInstance() {
    if (service == null) {
      service = new ColumnCompressorService();
      Iterator<ColumnCompressor> compressors = service.loader.iterator();
      while (compressors.hasNext()) {
        ColumnCompressor compressor = compressors.next();
        service.compressorTable.put(compressor.getVendor() + "." + compressor.getCompressorSet(),
            compressor.getClass());
      }
    }
    return service;
  }

  /**
   * Get a compressor object with the specified vendor and compressorSet name, if the compressor
   * class was loaded from CLASSPATH.
   *
   * @param vendor
   *          The vendor string of the compressor implementation, provided by the compressor class
   * @param compressorSet
   *          The compressor set string of the compressor implementation, provided by the compressor
   *          class
   * @return A ColumnCompressor implementation object
   */
  public ColumnCompressor getCompressor(String vendor, String compressorSet) {
    Class<? extends ColumnCompressor> compressorClass = compressorTable.get(vendor + "."
        + compressorSet);
    ColumnCompressor compressor = null;
    try {
      compressor = compressorClass.newInstance();
    } catch (Exception e) {
    }
    return compressor;
  }
}
