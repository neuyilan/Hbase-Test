package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
/**
 * 
 * @author qihouliang
 *
 */
public class ReadFieldsTests {
	public ReadFieldsTests(){
		
	}
	
	
	static String tableName = "orders";
	public static void main(String [] args ) throws IOException{
		Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.zookeeper.quorum", "lingcloud31,lingcloud29,data9");  
	    conf.set("hbase.zookeeper.property.clientPort", "2181");  
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable htable=new HTable(conf,tableName);
		
		FilterList filters = new FilterList();
	    RangeList list = new RangeList();
		
		boolean useIndex=true;
		boolean isPrint=true;
		double c3_start=200000;
		double c3_end=1000000;
		
		int c1_start=10000000;
		int c1_end=20000000;
		 long rowCount=0;
		 double sum=0;
		String c4_start="1990-01-01";
		String c4_end="2000-01-01";
		
		int c7=0;
		
		String c5="5-LOW";
		
		
		
//		
//		if (c3_end > 0) {
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),CompareOp.LESS, Bytes.toBytes(c3_end)));
////		list.addRange(new Range(Bytes.toBytes("f:c3"), Bytes.toBytes(c3_start), CompareOp.GREATER, Bytes.toBytes(c3_end),CompareOp.LESS));
//		list.addRange(new Range(Bytes.toBytes("f:c3"), null, CompareOp.NO_OP, Bytes.toBytes(c3_end),CompareOp.LESS));
//		}
//		if (c3_start >= 0) {
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),CompareOp.GREATER, Bytes.toBytes(c3_start)));
//		list.addRange(new Range(Bytes.toBytes("f:c3"), Bytes.toBytes(c3_start), CompareOp.GREATER, Bytes.toBytes(c3_end),CompareOp.LESS));
//		}
//	
//	if (c1_end > 0) {
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c1"),CompareOp.LESS, Bytes.toBytes(c1_end)));
////		list.addRange(new Range(Bytes.toBytes("f:c1"), Bytes.toBytes(c1_start), CompareOp.GREATER, Bytes.toBytes(c1_end),CompareOp.LESS));
//	}
//	if (c1_start >= 0) {
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c1"),CompareOp.GREATER, Bytes.toBytes(c1_start)));
//		list.addRange(new Range(Bytes.toBytes("f:c1"), Bytes.toBytes(c1_start), CompareOp.GREATER, Bytes.toBytes(c1_end),CompareOp.LESS));
//    }
	
//	filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c4"),CompareOp.LESS, Bytes.toBytes(c4_end)));
//	list.addRange(new Range(Bytes.toBytes("f:c4"), Bytes.toBytes(c4_start), CompareOp.GREATER, Bytes.toBytes(c4_end),CompareOp.LESS));
//	
//	filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c4"),CompareOp.GREATER, Bytes.toBytes(c4_start)));
//	list.addRange(new Range(Bytes.toBytes("f:c4"), Bytes.toBytes(c4_start), CompareOp.GREATER, Bytes.toBytes(c4_end),CompareOp.LESS));
	
	
//	filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c7"),CompareOp.EQUAL, Bytes.toBytes(c7)));
//	list.addRange(new Range(Bytes.toBytes("f:c7"), Bytes.toBytes(c7), CompareOp.EQUAL, null,CompareOp.NO_OP));
//	
	filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),CompareOp.EQUAL, Bytes.toBytes(c5)));
	list.addRange(new Range(Bytes.toBytes("f:c5"), Bytes.toBytes(c5), CompareOp.EQUAL, null,CompareOp.NO_OP));
		
		
		
		
		
		Scan scan = new Scan();
		if(useIndex){
			scan.setFilter(filters);
			scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Writables.getBytes(list));
//			scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes(true));
		}
		
		
		scan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(0.5));
		
		long startTime = System.currentTimeMillis();
		ResultScanner scanner=htable.getScanner(scan);
		
		double sumResult=0;
		int caching=1000000;
		boolean print=true;
		Result[] result = scanner.next(caching);
	      if (print) {
	        for (Result r : result) {
//	        	for(Cell cell:r.rawCells()){
//					if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c3")){
//						sum+=Bytes.toDouble(CellUtil.cloneValue(cell));
//					}
//	        	}
	          println(r);
	        }
	      }
	      long endTime = System.currentTimeMillis();
		  System.out.println("time skiped:"+(endTime-startTime)+"ms");
	      
	      
//		Result result = null;
//	    long startTime = System.currentTimeMillis();
//	    int count = 0;
//	    while ((result = scanner.next()) != null) {
//	      count++;
//	      if (print) {
//	        println(result);
//	      }
//	   }
//	   long endTime = System.currentTimeMillis();
//	   System.out.println("time skiped:"+(endTime-startTime)/1000+"s");
//	   System.out.println("total count:"+count);
	      
	      
//	      
//		for(Result r:scanner){
//			System.out.println("rowkey:"+Bytes.toString(r.getRow()));
//			for(Cell cell:r.rawCells()){
//				if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c3")){
////					System.out.println("the cloneFamily:"+Bytes.toString(CellUtil.cloneFamily(cell)));
//					System.out.println("the cloneQualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
//					System.out.println("the cloneValue:"+Bytes.toDouble(CellUtil.cloneValue(cell)));
//					sumResult+=Bytes.toDouble(CellUtil.cloneValue(cell));
//				}
//				if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c5")){
////					System.out.println("the cloneFamily:"+Bytes.toString(CellUtil.cloneFamily(cell)));
//					System.out.println("the cloneQualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
//					System.out.println("the cloneValue:"+Bytes.toString(CellUtil.cloneValue(cell)));
//				}
//			}
//		}
		
//		System.out.println("sumResult"+sumResult);
		
//		if(admin.tableExists(tableName)){
//			try{
//				admin.disableTable(tableName);
//				HTableDescriptor htd=admin.getTableDescriptor(Bytes.toBytes(tableName));
//				HColumnDescriptor [] tcds=htd.getColumnFamilies();
//				for(int i=0;i<tcds.length;i++)
//				{
//					HColumnDescriptor hcd=tcds[i];
////					DataInput in=new DataInputBuffer();
////					hcd.readFields(in);
//					System.out.println("tcds[i].toString();-------->:"+hcd.toString());
//				}
//				
//
//				System.out.println("htd.toString()-------->:"+htd.toString());
//				admin.enableTable(tableName);
//			}catch(Exception e){
//			e.printStackTrace();
//			}
//			
//		}
		htable.close();
		admin.close();
	}
	
	
	static void println(Result result) {
	    StringBuilder sb = new StringBuilder();
	    sb.append("row=" + Bytes.toString(result.getRow()));

	    List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
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
