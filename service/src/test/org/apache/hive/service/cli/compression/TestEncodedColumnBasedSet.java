package org.apache.hive.service.cli.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;


import org.apache.hive.service.cli.Column;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.compression.EncodedColumnBasedSet;
import org.apache.hive.service.cli.thrift.TRowSet;


import org.apache.hadoop.hive.conf.HiveConf;


import java.io.IOException;




/*
 * To test functionality of EncodedColumnBasedSets. 
 */
public class TestEncodedColumnBasedSet {

  private TRowSet testTRowSet;
  private static HiveConf hiveConf = new HiveConf();
  
  /**
   * sets up the pre-requisite data variables. throws JSONException.
   */
  @Before
  public void setUp() throws JSONException {
    testTRowSet = new TRowSet();
    testTRowSet.setStartRowOffset((long) 0);
    Column column = new Column(Type.INT_TYPE);
    column.addValue(Type.INT_TYPE, 0);
    column.addValue(Type.INT_TYPE, 1);
    column.addValue(Type.INT_TYPE, 2);
    testTRowSet.addToColumns(column.toTColumn());
   
    hiveConf.setBoolean("hive.resultset.compression.enabled", true);
    
  }

  /**
   * send the right information as part of compressorInfo.
   * EncodedColumnBasedSet should compress the data.
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void testCompressionUsingSnappy() throws IOException, JSONException {
    
    JSONObject temp = new JSONObject();
    temp.put("vendor", "snappy");
    temp.put("compressorSet", "snappy");
    JSONObject compressorInfo = new JSONObject();
    compressorInfo.put("INT_TYPE", temp);
    hiveConf.set("CompressorInfo", compressorInfo.toString());
    EncodedColumnBasedSet ecbs = new EncodedColumnBasedSet(testTRowSet);
    ecbs.setConf(hiveConf);
    TRowSet compressed = ecbs.toTRowSet();
    byte[] compressedData = compressed.getEnColumns().get(0).getEnData();
    assertArrayEquals(SnappyIntCompressor.decompress(compressedData), new int[] { 0, 1, 2 });
  }

  /**
   * put the wrong information in compressorInfo. EncodedColumnBasedSet.
   * should send back the column uncompressed.
   * @throws IOException.
   * @throws JSONException.
   */
  @Test
  public void testCompressionNoOp() throws IOException, JSONException {
  
    JSONObject temp = new JSONObject();
    temp.put("vendor", "thisiswrong");
    temp.put("compressorSet", "thisiswrong");
    JSONObject compressorInfo = new JSONObject();
    compressorInfo.put("INT_TYPE", temp);
    hiveConf.set("CompressorInfo", compressorInfo.toString());
    EncodedColumnBasedSet ecbs = new EncodedColumnBasedSet(testTRowSet);
    ecbs.setConf(hiveConf);
    TRowSet compressed = ecbs.toTRowSet();
    assertEquals(ecbs.getColumns().size(), 1);
    assertEquals(compressed.getEnColumnsSize(), 0);
    Integer[] ints = new Integer[] { 0, 1, 2 };
    assertArrayEquals(compressed.getColumns().get(0).getI32Val().getValues().toArray(), ints);
  }
}
