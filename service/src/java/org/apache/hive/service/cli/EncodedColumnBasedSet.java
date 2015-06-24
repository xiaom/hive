package org.apache.hive.service.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.hive.service.cli.thrift.TEnColumn;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.resultset.compression.ColumnCompressor;
import org.apache.hive.service.resultset.compression.ColumnCompressorService;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.BitSet;
import java.nio.ByteBuffer;

public class EncodedColumnBasedSet extends ColumnBasedSet {
  /**
   * Class that can handle resultSets which are a mix of compressed and uncompressed columns To
   * compress columns, it would need a JSON string a client would send(ThriftCLIService.java) The
   * JSON string would have vendor, compressorSet and the entryClass (for a plugin) Using them,
   * server would know which entry class to call. A All of these entry classes would have to
   * implement the interface (ColumnCompressor.java) The compressorInfo is the JSON string in the
   * form {"INT_TYPE": {"vendor": (name), "compressorSet":(name), "entryClass": (name)}, ...} If the
   * client mentioned the wrong vendor or classname or compressorSet, or it does so correctly but
   * the server does not have it, the column would not be compressed The client can find out which
   * columns are/aren't compressed by looking at the compressorBitmap The column number that's
   * compressed would have it's respective bit set and vice-versa
   */

  private HiveConf hiveConf;
  private boolean isCompression; //flag to indicate if compression is happening or not. depends on CompressorInfo, and 
  //the settings in hive-site.xml
  
  private String compressorInfo;
  
  /*
   * Compressors that shouldn't be used specified as csv under
   * "hive.resultset.compression.disabled.compressors".
   */
  private HashSet<String> disabledCompressors = new HashSet<String>();

  public EncodedColumnBasedSet(TableSchema schema) {
    super(schema);
  }

  
  public EncodedColumnBasedSet(TRowSet tRowSet) {
    super(tRowSet);
  }

  public EncodedColumnBasedSet(Type[] types, List<Column> subset, long startOffset) {
    super(types, subset, startOffset);
  }

  /**
   * Given an index, update the TRowSet with the column (uncompressed) and update the bitmap with
   * that index set to false.
   * 
   * @param tRowSet
   *          a given TRowSet
   * @param i
   *          index for the column which needs to be inserted into TColumns
   * @param bitmap
   *          bitmap that needs to be updated with info that column "i" is not compressed
   * 
   */

  private void addUncompressedColumn(TRowSet tRowSet, int i, BitSet bitmap) {
    tRowSet.addToColumns(columns.get(i).toTColumn());
    bitmap.set(i, false);
  }

  /**
   * Main function that converts the columns in the RowSet if the compressorInfo points to a valid
   * class and the compressor is not part of the disable compressorList (referred to above).
   *
   */
  /*
  private void setCompressorInfo(String s) {
	  compressorInfo = s;
  }
  
  public String getCompressorInfo() {
	  return compressorInfo;
  }
  */
  
  @Override
  public TRowSet toTRowSet() {
    //System.out.println(hiveConf.get("CompressorInfo"));
	//setCompressorInfo(hiveConf.get("CompressorInfo"));
    if (hiveConf == null) {
      throw new IllegalStateException("Hive configuration from session not set");
    }
    TRowSet tRowSet = new TRowSet(getStartOffset(), new ArrayList<TRow>());
    tRowSet.setColumns(new ArrayList<TColumn>());
    // A bitmap showing whether each column is compressed or not
    BitSet compressorBitmap = new BitSet();
    // Get the JSON string specifying client compressor preferences
    String compressorInfo = hiveConf.get("CompressorInfo");
    if (!hiveConf.getBoolVar(ConfVars.HIVE_RESULTSET_COMPRESSION_ENABLED) || compressorInfo == null) {
      for (int i = 0; i < columns.size(); i++) {
        addUncompressedColumn(tRowSet, i, compressorBitmap);
      }
    } else {
      JSONObject compressorInfoJSON = null;
      try {
        compressorInfoJSON = new JSONObject(compressorInfo);
      } catch (JSONException e) {
      }
      if (compressorInfoJSON != null) {
        for (int i = 0; i < columns.size(); i++) {
          isCompression = true;
          // Add this column, possibly compressed, to the row set.
          addColumn(tRowSet, i, compressorInfoJSON, compressorBitmap);
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
   * Add the column at specified index to the row set; compress it before adding if the JSON
   * configuration is valid.
   * 
   * @param tRowSet
   *          Row set to add the column to
   * @param i
   *          Index of the column of add to the row set
   * @param compressorInfoJson
   *          A JSON object containing type-specific compressor configurations
   * @param compressorBitmap
   *          The bitmap to be set to show whether the column is compressed or not
   */
  private void addColumn(TRowSet tRowSet, int i, JSONObject compressorInfoJson,
      BitSet compressorBitmap) {
    String vendor = "";
    String compressorSet = "";
    try {
      JSONObject jsonObject = compressorInfoJson.getJSONObject(columns.get(i).getType().toString());
      vendor = jsonObject.getString("vendor");
      compressorSet = jsonObject.getString("compressorSet");
    } catch (JSONException e) {
      addUncompressedColumn(tRowSet, i, compressorBitmap);
      return;
    }
    String compressorKey = vendor + "." + compressorSet;
    if (disabledCompressors.contains(compressorKey) == true) {
      addUncompressedColumn(tRowSet, i, compressorBitmap);
      return;
    }
    ColumnCompressor columnCompressorImpl = null;
    // Dynamically load the compressor class specified by the JSON configuration, if
    // the compressor jar is present in CLASSPATH.
    columnCompressorImpl = ColumnCompressorService.getInstance().getCompressor(vendor,
        compressorSet);
    if (columnCompressorImpl == null) {
      addUncompressedColumn(tRowSet, i, compressorBitmap);
      return;
    }
    // The compressor may not be able to compress the column because the type is not supported, etc.
    if (!columnCompressorImpl.isCompressible(columns.get(i))) {
      addUncompressedColumn(tRowSet, i, compressorBitmap);
      return;
    }
    // Create the TEnColumn that will be carrying the compressed column data
    TEnColumn tEnColumn = new TEnColumn();
    int colSize = columns.get(i).size();
    tEnColumn.setSize(colSize);
    tEnColumn.setType(columns.get(i).getType().toTType());
    tEnColumn.setNulls(columns.get(i).getNulls());
    tEnColumn.setCompressorName(compressorSet);
    compressorBitmap.set(i, true);
    if (colSize == 0) {
      tEnColumn.setEnData(new byte[0]);
    } else {
      tEnColumn.setEnData(columnCompressorImpl.compress(columns.get(i)));
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
   * Pass the Hive session configuration containing client compressor configurations to this object.
   * 
   * @param conf
   *          Current Hive session configuration to set
   */
  public void setConf(HiveConf conf) {
    this.hiveConf = conf;
    String[] disabledCompressors = this.hiveConf.getVar(
        ConfVars.HIVE_RESULTSET_COMPRESSION_DISABLED_COMPRESSORS).split(",");
    this.disabledCompressors.addAll(Arrays.asList(disabledCompressors));
  }
}
