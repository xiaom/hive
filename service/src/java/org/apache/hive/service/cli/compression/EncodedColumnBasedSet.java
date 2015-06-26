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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.Column;
import org.apache.hive.service.cli.ColumnBasedSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TEnColumn;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Column based set that compresses the columnar result set, with the compression plugin specified
 * by client preferences, if such a plugin exits.
 *
 * The client specifies a compression plugin by passing a JSON string of the format {"INT_TYPE":
 * {"vendor": <vendor name>, "compressorSet":<compressor set name>}, ...} with the key
 * "CompressorInfo" as a client configuration. Hive server then looks up the class that
 * implements ColumnCompressor with the specified vendor and compressorSet strings. If such a class
 * is found, it is used to compress columnar result set. If not, uncompressed column result set
 * will be returned. The client can find out which columns are/aren't compressed by looking at the
 * compressorBitmap field. The column that's compressed would have its corresponding bit set
 * and vice-versa.
 * 
 */
public class EncodedColumnBasedSet extends ColumnBasedSet {

  private HiveConf hiveConf;

  private final Log LOG = LogFactory.getLog(EncodedColumnBasedSet.class.getName());

  /**
   * Compressors that shouldn't be used specified as csv under
   * "hive.resultset.compression.disabled.compressors".
   */
  private Set<String> disabledCompressors = new HashSet<String>();

  public EncodedColumnBasedSet(TableSchema schema) {
    super(schema);
  }

  public EncodedColumnBasedSet(TRowSet trowset) {
    super(trowset);
  }

  public EncodedColumnBasedSet(Type[] types, List<Column> subset, long startOffset) {
    super(types, subset, startOffset);
  }

  /**
   * Main function that compresses the columns in the RowSet if the compressorInfo points to a
   * valid class and the compressor is not part of the disable compressorList (referred to
   * above).
   *
   */
  @Override
  public TRowSet toTRowSet() {

    if (hiveConf == null) {
      throw new IllegalStateException("Hive configuration from session not set");
    }
    TRowSet tRowSet = new TRowSet(getStartOffset(), new ArrayList<TRow>());
    tRowSet.setColumns(new ArrayList<TColumn>());
    // A bitmap showing whether each column is compressed or not
    BitSet compressorBitmap = new BitSet();
    // Get the JSON string specifying client compressor preferences
    String compressorInfo = hiveConf.get("CompressorInfo");
    if (!hiveConf.getBoolVar(ConfVars.HIVE_RESULTSET_COMPRESSION_ENABLED) 
        || compressorInfo == null) {
      for (int i = 0; i < columns.size(); i++) {
        addUncompressedColumn(tRowSet, i, compressorBitmap);
      }
    } else {
      JSONObject compressorInfoJson = null;
      try {
        compressorInfoJson = new JSONObject(compressorInfo);
      } catch (JSONException e) {
        LOG.info("JSON string malformed " + e.toString());
      }
      if (compressorInfoJson != null) {
        for (int i = 0; i < columns.size(); i++) {
          // Add this column, possibly compressed, to the row set.
          addColumn(tRowSet, i, compressorInfoJson, compressorBitmap);
        }
      } else {
        for (int i = 0; i < columns.size(); i++) {
          addUncompressedColumn(tRowSet, i, compressorBitmap);
        }
      }
    }
    ByteBuffer bitmap = ByteBuffer.wrap(compressorBitmap.toByteArray());
    tRowSet.setCompressorBitmap(bitmap);
    return tRowSet;
  }

  /**
   * Given an index, update the TRowSet with the column (uncompressed) and update the bitmap with
   * that index set to false.
   *
   * @param trowset
   *          a given TRowSet
   * @param index
   *          index for the column which needs to be inserted into TColumns
   * @param bitmap
   *          bitmap that needs to be updated with info that column "i" is not compressed
   *
   */

  private void addUncompressedColumn(TRowSet trowset, int index, BitSet bitmap) {
    trowset.addToColumns(columns.get(index).toTColumn());
    bitmap.set(index, false);
  }

  /**
   * Add the column at specified index to the row set; compress it before adding if the JSON
   * configuration is valid.
   *
   * @param tRowSet
   *          Row set to add the column to
   * @param index
   *          Index of the column of add to the row set
   * @param compressorInfoJson
   *          A JSON object containing type-specific compressor configurations
   * @param compressorBitmap
   *          The bitmap to be set to show whether the column is compressed or not
   */
  private void addColumn(TRowSet tRowSet, int index, JSONObject compressorInfoJson,
      BitSet compressorBitmap) {
    String vendor = "";
    String compressorSet = "";
    try {
      JSONObject jsonObject = compressorInfoJson.getJSONObject(columns.get(index).getType()
          .toString());
      vendor = jsonObject.getString("vendor");
      compressorSet = jsonObject.getString("compressorSet");
    } catch (JSONException e) {
      addUncompressedColumn(tRowSet, index, compressorBitmap);
      return;
    }
    String compressorKey = vendor + "." + compressorSet;
    if (disabledCompressors.contains(compressorKey)) {
      addUncompressedColumn(tRowSet, index, compressorBitmap);
      return;
    }
    ColumnCompressor columnCompressorImpl = null;
    // Dynamically load the compressor class specified by the JSON configuration, if
    // the compressor jar is present in CLASSPATH.
    columnCompressorImpl = ColumnCompressorService.getInstance().getCompressor(vendor,
        compressorSet);
    if (columnCompressorImpl == null) {
      addUncompressedColumn(tRowSet, index, compressorBitmap);
      return;
    }
    // The compressor may not be able to compress the column because the type is not supported,
    // etc.
    if (!columnCompressorImpl.isCompressible(columns.get(index))) {
      addUncompressedColumn(tRowSet, index, compressorBitmap);
      return;
    }
    // Create the TEnColumn that will be carrying the compressed column data
    TEnColumn tEnColumn = new TEnColumn();
    int colSize = columns.get(index).size();
    tEnColumn.setSize(colSize);
    tEnColumn.setType(columns.get(index).getType().toTType());
    tEnColumn.setNulls(columns.get(index).getNulls());
    tEnColumn.setCompressorName(compressorSet);
    compressorBitmap.set(index, true);
    if (colSize == 0) {
      tEnColumn.setEnData(new byte[0]);
    } else {
      tEnColumn.setEnData(columnCompressorImpl.compress(columns.get(index)));
    }
    tRowSet.addToEnColumns(tEnColumn);
  }

  @Override
  public EncodedColumnBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<Column> subset = new ArrayList<Column>();
    for (int i = 0; i < columns.size(); i++) {
      subset.add(columns.get(i).extractSubset(0, numRows));
    }
    EncodedColumnBasedSet result = new EncodedColumnBasedSet(types, subset, startOffset);
    startOffset += numRows;
    return result;
  }

  /**
   * Pass the Hive session configuration containing client compressor preferences to this object.
   *
   * @param conf
   *          Current Hive session configuration to set
   */
  public void setConf(HiveConf conf) {
    this.hiveConf = conf;
    String[] disabled = this.hiveConf.getVar(
        ConfVars.HIVE_RESULTSET_COMPRESSION_DISABLED_COMPRESSORS).split(",");
    this.disabledCompressors.addAll(Arrays.asList(disabled));
  }
}
