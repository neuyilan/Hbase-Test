package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.BigDecimalColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.IntegerColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

public class QueryThreeTest {
	
	static Configuration conf = HBaseConfiguration.create();
	
	FilterList filters = new FilterList();
    RangeList list = new RangeList();
     
	public QueryThreeTest(){
		conf.addResource("hbase-site.xml");
		conf.set("hbase.zookeeper.quorum", "lingcloud31,lingcloud29,data9");  
	    conf.set("hbase.zookeeper.property.clientPort", "2181");  
	}
	
	
	String tableName = "test";
	boolean useIndex=true;
	boolean isPrint=true;
	
	double c3_start=200000;
	double c3_end=900000;
	double c3_equal=200000.01;
	
	int c1_start=10000000;
	int c1_end=20000000;
	
	static long rowCount=0;
	static double sum=0;
	static double max=0;
	static double min=0;
	
	String c4_start="1880-01-01";
	String c4_end="2015-01-01";
	
	int c7=0;
	
	String c5="5-LOW";
	
	
	
	
    // key ORDERKEY Int
    // c1 CUSTKEY Int
    // c2 ORDERSTATUS String
    // c3 TOTALPRICE Double index
    // c4 ORDERDATE String index
    // c5 ORDERPRIORITY String index
    // c6 CLERK String
    // c7 SHIPPRIORITY Int
    // c8 COMMENT String
	
	
	/**
	 * select count(*),sum(c3),max(c3),min(c3) from orders where c3>200000 and c3<900000;
	 * select count(*),sum(c3),max(c3),min(c3) from orders where c1>10000000 and c1<20000000;
	 * @throws Throwable 
	 */
	public  void queryTwo() throws Throwable{
		Table table=new HTable(conf,tableName);
		Scan scan = new Scan();
		
		
		// set the filter
//		if (c3_end > 0) {
//			filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),CompareOp.LESS, Bytes.toBytes(c3_end)));
//			//list.addRange(new Range(Bytes.toBytes("f:c3"), Bytes.toBytes(c3_start), CompareOp.GREATER, Bytes.toBytes(c3_end),CompareOp.LESS));
//        }
//		if (c3_start >= 0) {
//			filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),CompareOp.GREATER, Bytes.toBytes(c3_start)));
//			//list.addRange(new Range(Bytes.toBytes("f:c3"), Bytes.toBytes(c3_start), CompareOp.GREATER, Bytes.toBytes(c3_end),CompareOp.LESS));
//        }

		
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),CompareOp.EQUAL, Bytes.toBytes(c3_equal)));
//		list.addRange(new Range(Bytes.toBytes("f:c5"), Bytes.toBytes(c3_equal), CompareOp.EQUAL, null,CompareOp.NO_OP));
		
		if (c1_end > 0) {
			filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c1"),CompareOp.LESS, Bytes.toBytes(c1_end)));
			list.addRange(new Range(Bytes.toBytes("f:c1"), Bytes.toBytes(c1_start), CompareOp.GREATER, Bytes.toBytes(c1_end),CompareOp.LESS));
		}
		if (c1_start >= 0) {
			filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c1"),CompareOp.GREATER, Bytes.toBytes(c1_start)));
			list.addRange(new Range(Bytes.toBytes("f:c1"), Bytes.toBytes(c1_start), CompareOp.GREATER, Bytes.toBytes(c1_end),CompareOp.LESS));
	    }
//		
		
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c4"),CompareOp.LESS, Bytes.toBytes(c4_end)));
//		list.addRange(new Range(Bytes.toBytes("f:c4"), null, CompareOp.NO_OP, Bytes.toBytes(c4_end),CompareOp.LESS));
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c4"),CompareOp.GREATER, Bytes.toBytes(c4_start)));
//		list.addRange(new Range(Bytes.toBytes("f:c4"), null, CompareOp.GREATER, Bytes.toBytes(c4_start),CompareOp.NO_OP));
		
		
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c7"),CompareOp.EQUAL, Bytes.toBytes(c7)));
//		list.addRange(new Range(Bytes.toBytes("f:c7"), Bytes.toBytes(c7), CompareOp.EQUAL, null,CompareOp.NO_OP));
		
//		filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),CompareOp.EQUAL, Bytes.toBytes(c5)));
//		list.addRange(new Range(Bytes.toBytes("f:c5"), Bytes.toBytes(c5), CompareOp.EQUAL, null,CompareOp.NO_OP));
		
		if(useIndex){
			scan.setFilter(filters);
			scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Writables.getBytes(list));
//			scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes(true));
		}
		 
		scan.addFamily(Bytes.toBytes("f"));
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
		
//		 byte[] colFamily = scan.getFamilies()[0];
//	      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
//	      System.out.println("qualifiers"+qualifiers.toString());
//	      byte[] qualifier = null;
//	      if (qualifiers != null && !qualifiers.isEmpty()) {
//	        qualifier = qualifiers.pollFirst();
//	        System.out.println("qualifier"+Bytes.toString(qualifier));
//	      }
//	      System.out.println("colFamily"+Bytes.toString(colFamily));
	      
	      
	      
	      
		//use the aggregate function
//		AggregationClient client=new AggregationClient(conf);
////		rowCount=client.rowCount(table, new LongColumnInterpreter(), scan);
//		System.out.println(client.toString());
//		System.out.println(scan.toString());
////		sum=client.sum(table, new DoubleColumnInterpreter(), scan);
//		max=client.max(table, new DoubleColumnInterpreter(), scan);
////		min=client.min(table, new DoubleColumnInterpreter(), scan);
//		client.close();
//		table.close();
		
	}
	
	
	
	
	public static void main(String[] args) throws Throwable{
//		QueryThreeTest test=new QueryThreeTest();
//		long start=System.currentTimeMillis();
//		
////		//test the queryTwo();
//		test.queryTwo();
//		long end =System.currentTimeMillis();
//		System.out.println("Final Time elapsed:"+(end-start)/1000+"s");
//		System.out.println("total num count:"+rowCount);
//		System.out.println("sum:"+sum);
//		System.out.println("max:"+max);
//		System.out.println("min:"+min);
		
		
		
//		String tableName1="test";
//		Table table1=new HTable(conf,tableName1);
//		Configuration conf = table1.getConfiguration();
//		conf.addResource("hbase-site.xml");
//		conf.set("hbase.zookeeper.quorum", "lingcloud31,lingcloud29,data9");  
//	    conf.set("hbase.zookeeper.property.clientPort", "2181");  
//	    AggregationClient aggregationClient=new AggregationClient(conf);
//	    Scan scan = new Scan();
//	    scan.addFamily(Bytes.toBytes("f"));
//		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
//		long start=System.currentTimeMillis();
//		System.out.println("start:"+start);
//		sum=aggregationClient.sum(table1, new DoubleColumnInterpreter(), scan);
//		long end =System.currentTimeMillis();
//		System.out.println("end:"+end);
//		System.out.println("sum:"+sum);
//		System.out.println("end-start:"+(end-start));
//		aggregationClient.close();
		
		
		
		
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
		conf.set("hbase.zookeeper.quorum", "lingcloud31,lingcloud29,data9");  
	    conf.set("hbase.zookeeper.property.clientPort", "2181");  
	    Table table=new HTable(conf,"test");
	    Scan scan = new Scan();
	    FilterList filters = new FilterList();
	    int c1_start=10000000;
	    int c1_end=20000000;
	    
	    filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c1"),CompareOp.GREATER, Bytes.toBytes(c1_start)));
	    RangeList list = new RangeList();
	    list.addRange(new Range(Bytes.toBytes("f:c1"), Bytes.toBytes(c1_start), CompareOp.GREATER, Bytes.toBytes(c1_end),CompareOp.LESS));
	    scan.addFamily(Bytes.toBytes("f"));
//	    scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
		
		scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Writables.getBytes(list));
//		scan.setFilter(filters);
		
		AggregationClient client=new AggregationClient(conf);
		sum=client.sum(table, new DoubleColumnInterpreter(), scan);
		max=client.max(table, new DoubleColumnInterpreter(), scan);
		min=client.min(table, new DoubleColumnInterpreter(), scan);
		rowCount=client.rowCount(table, new DoubleColumnInterpreter(), scan);
		client.close();
		table.close();
		System.out.println("max:"+max);
		System.out.println("rowCount:"+rowCount);
		System.out.println("min:"+min);
		System.out.println("sum:"+sum);
		
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
