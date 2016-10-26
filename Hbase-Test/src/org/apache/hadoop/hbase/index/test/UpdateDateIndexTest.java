package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class UpdateDateIndexTest {
	public UpdateDateIndexTest() {

	}

	public static void main(String[] args) throws IOException {
//		update();
		readMulVersion();
	}

	public static void update() throws IOException {
		String tableName = "orders_withindex";
		Configuration conf = HBaseConfiguration.create();
		String c5_equal = "2-HIGH";

//		FilterList filters = new FilterList();
//		RangeList list = new RangeList();
//
//		if (c5_equal != null) {
//			filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"),
//					Bytes.toBytes("c5"), CompareOp.EQUAL, Bytes
//							.toBytes(c5_equal)));
//			list.addRange(new Range(Bytes.toBytes("f:c5"), Bytes
//					.toBytes(c5_equal), CompareOp.EQUAL, null, CompareOp.NO_OP));
//		}

		Put put = new Put(Bytes.toBytes("99000008"));
		
		String value = "irindex first change-------";
		put.add(Bytes.toBytes("f"), Bytes.toBytes("c8"), Bytes.toBytes(value));

		HTable table = new HTable(conf, tableName);
		table.put(put);
		table.close();
	}

	public static void readMulVersion() throws IOException {
		String tableName = "orders_withindex";
		Configuration conf = HBaseConfiguration.create();
		Get get = new Get(Bytes.toBytes("99000008"));
		get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
		get.setMaxVersions(4);

		HTable table = new HTable(conf, tableName);
		Result result = table.get(get);
		List<Cell> list=result.listCells();
		StringBuilder sb=new StringBuilder();
		for(Cell cell:list){
			String rowkey=new String(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength(),"UTF-8");
			sb.append("row="+rowkey);
			
			String family=new String(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength(),"UTF-8");
			sb.append(",family="+family);
			
			String qualifier=new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength(),"UTF-8");
			sb.append(",qualifier="+qualifier);
			
			String value=new String(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength(),"UTF-8");
			sb.append(",value="+value);
			long timestamp=cell.getTimestamp();
			sb.append(",timestamp="+timestamp);
			sb.append("\n");
		}
//		println(result);
		System.out.println(sb);

		table.close();

	}

	static void println(Result result) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=" + Bytes.toString(result.getRow()));

		List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"),
				Bytes.toBytes("c1"));
		if (kv.size() != 0) {
			sb.append(", f:c1=" + Bytes.toInt(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
		if (kv.size() != 0) {
			sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
		if (kv.size() != 0) {
			sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
		if (kv.size() != 0) {
			sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
		if (kv.size() != 0) {
			sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
		if (kv.size() != 0) {
			sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
		if (kv.size() != 0) {
			sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
		if (kv.size() != 0) {
			sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
		}
		System.out.println(sb.toString());
	}
}
