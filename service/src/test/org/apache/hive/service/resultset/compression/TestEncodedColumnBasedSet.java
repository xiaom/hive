package org.apache.hive.service.resultset.compression;

import static org.junit.Assert.*;
import java.io.IOException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.Column;
import org.apache.hive.service.cli.EncodedColumnBasedSet;
import org.apache.hive.service.cli.Type;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

/*
 * To test functionality of EncodedColumnBasedSets. 
 */
public class TestEncodedColumnBasedSet {

  private static TRowSet testTRowSet;
  private static Column testColumn;
  private static EncodedColumnBasedSet ecbs;
  private static HiveConf hiveConf = new HiveConf();
  private static JSONObject info;
  private static JSONObject stringJSON;

  @Before
  public void setUp() throws JSONException {
    // create a TRowSet with one Column, which is of type INT. and has
    // values {0, 1, 2}. and
    testTRowSet = new TRowSet();
    testTRowSet.setStartRowOffset((long) 0);
    testColumn = new Column(Type.INT_TYPE);
    info = new JSONObject();
    stringJSON = new JSONObject();
    testColumn.addValue(Type.INT_TYPE, 0);
    testColumn.addValue(Type.INT_TYPE, 1);
    testColumn.addValue(Type.INT_TYPE, 2);
    testTRowSet.addToColumns(testColumn.toTColumn());
  }

  @Test
  public void positive() throws IOException, JSONException {
    // send the right information as part of compressorInfo.
    // EncodedColumnBasedSet should compress the data.
    info.put("vendor", "org.apache.hive.service.cli");
    info.put("compressorSet", "pluginsnappy");
    stringJSON.put("INT_TYPE", info);
    hiveConf.set("CompressorInfo", stringJSON.toString());
    ecbs = new EncodedColumnBasedSet(testTRowSet);
    ecbs.setConf(hiveConf);
    TRowSet compressed = ecbs.toTRowSet();
    assertEquals(ecbs.getColumns().size(), 1);
    byte[] compressedData = compressed.getEnColumns().get(0).getEnData();
    assertArrayEquals(SnappyIntCompressor.decompress(compressedData), new int[] { 0, 1, 2 });
  }

  @Test
  public void negative() throws IOException, JSONException {
    // put the wrong information in compressorInfo. EncodedColumnBasedSet
    // should send back the column uncompressed.
    info.put("vendor", "org.apache.hive.service.cli");
    info.put("compressorSet", "thisiswrong");
    stringJSON.put("INT_TYPE", info);
    hiveConf.set("CompressorInfo", stringJSON.toString());
    ecbs = new EncodedColumnBasedSet(testTRowSet);
    ecbs.setConf(hiveConf);
    TRowSet compressed = ecbs.toTRowSet();
    assertEquals(ecbs.getColumns().size(), 1);
    assertEquals(compressed.getEnColumnsSize(), 0);
    Integer[] l = new Integer[] { 0, 1, 2 };
    assertArrayEquals(compressed.getColumns().get(0).getI32Val().getValues().toArray(), l);
  }
}
