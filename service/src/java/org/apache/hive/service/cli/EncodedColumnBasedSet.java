package org.apache.hive.service.cli;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.TEnColumn;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TRow;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.BitSet;
import java.nio.ByteBuffer;

public class EncodedColumnBasedSet extends ColumnBasedSet {
	private String compressorInfo;
	public EncodedColumnBasedSet(TableSchema schema) {
		super(schema);
	}
	
	public EncodedColumnBasedSet(TRowSet tRowSet) {
		super(tRowSet);
	}

	public EncodedColumnBasedSet(Type[] types, List<Column> subset,
			long startOffset) {
		super(types, subset, startOffset);
	}

	private static final byte[] MASKS = new byte[] {
		0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte)0x80
	};

	public static byte[] toBinary(BitSet bitset) {
	byte[] nulls = new byte[1 + (bitset.length() / 8)];
	for (int i = 0; i < bitset.length(); i++) {
	  nulls[i / 8] |= bitset.get(i) ? MASKS[i % 8] : 0;
	}
	return nulls;
	}

	@Override
	public TRowSet toTRowSet() {
		TRowSet tRowSet = new TRowSet(getStartOffset(), new ArrayList<TRow>());
		ColumnCompressor columnCompressorImpl = null;
		JSONObject jsonContainer = null;		
		BitSet compressorBitmap = new BitSet();
		try {
			jsonContainer = new JSONObject(this.compressorInfo);
		}
		catch(JSONException e){}

		for(int i = 0; i < columns.size(); i++) {

			String vendor = "";
			String compressorSet = "";
			String entryClass = "";
			
			try {
				JSONObject jsonObject = jsonContainer.getJSONObject(columns.get(i).getType().toString());
	        	vendor = jsonObject.getString("vendor");
	        	compressorSet = jsonObject.getString("compressorSet");
	        	entryClass = jsonObject.getString("entryClass");
			}
			catch(JSONException e){}
			String compressorName = vendor+"."+compressorSet;
			try {
				columnCompressorImpl = (ColumnCompressor) Class.forName(compressorName+"."+entryClass).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				//System.out.println("Not Compressing!");
				tRowSet.addToColumns(columns.get(i).toTColumn());

				//System.out.format("number of rows is %d\n", columns.get(i).size());
				compressorBitmap.set(i, false);
				continue;
			}

			TEnColumn tEnColumn = new TEnColumn();
			int size = columns.get(i).size();
			tEnColumn.setSize(size);
			tEnColumn.setType(columns.get(i).getType().toTType());			
			tEnColumn.setNulls(columns.get(i).getNulls());
			tEnColumn.setCompressorName(compressorSet);
			compressorBitmap.set(i, true);
			if(size == 0)
				tEnColumn.setEnData(new byte[0]);
			else {
				tEnColumn.setEnData(columnCompressorImpl.compress(columns.get(i)));
			}
				
			tRowSet.addToEnColumns(tEnColumn);
		}

		ByteBuffer bitmap = ByteBuffer.wrap(toBinary(compressorBitmap));
		//System.out.println(compressorBitmap);
		tRowSet.setCompressorBitmap(bitmap);
		return tRowSet;
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
	
	@Override
	public RowSet setArgs(String... args){
		this.compressorInfo = args[0];
		return this;
	}
}
