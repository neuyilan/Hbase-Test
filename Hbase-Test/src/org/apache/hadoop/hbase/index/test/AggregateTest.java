package org.apache.hadoop.hbase.index.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import com.google.protobuf.ServiceException;

/**
 * 
 * @author qihouliang
 *
 */
public class AggregateTest {
	private static final byte[] FAMILY = Bytes.toBytes("f");
	private static final byte[] QUALIFIER = Bytes.toBytes("c3");
	private static final byte[] START_KEY = Bytes.toBytes("4");
	private static final byte[] STOP_KEY = Bytes.toBytes("6");
//	private static byte[] ROW = Bytes.toBytes("1");
	private static final int ROWSIZE = 20;
//	private static byte[][] ROWS = makeN(ROW, ROWSIZE);
	
	
	 // key ORDERKEY Int
	
    // c1 CUSTKEY Int
    // c2 ORDERSTATUS String
    // c3 TOTALPRICE Double index
    // c4 ORDERDATE String index
    // c5 ORDERPRIORITY String index
    // c6 CLERK String
    // c7 SHIPPRIORITY Int
    // c8 COMMENT String
	
	
	
	//select max(c3) ,count(*) ,c5 as cnt ,sum(c3) from orders where 200000<c3 and c3<900000 group by c5 order by cnt limit 100;
	
	
	
	public static void main(String []args) throws ServiceException, Throwable{
		
			long start=System.currentTimeMillis();
			Configuration conf = HBaseConfiguration.create();
//			conf.set("hbase.zookeeper.quorum", "lingcloud31,lingcloud29,data9");  
//		    conf.set("hbase.zookeeper.property.clientPort", "2181");  
			String tableName = "orders_noindex";
			HBaseAdmin admin = new HBaseAdmin(conf);
			Table table=new HTable(conf,tableName);
			/***************************************************/
			boolean useIndex=false;
			String c5_equal = "5-LOW";
			double c3_low = 10;
			double c3_top = 900100;
			int count=0;
			String date="1995-07-16";
			String status="F";
			  Scan scan = new Scan();
		      FilterList filters = new FilterList();
		      RangeList list = new RangeList();
	
		      if (c5_equal != null) {
		        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
		            CompareOp.EQUAL, Bytes.toBytes(c5_equal)));
		        list.addRange(new Range(Bytes.toBytes("f:c5"), Bytes.toBytes(c5_equal), CompareOp.EQUAL,
		          null, CompareOp.NO_OP));
		      }
		      
//		      if (status != null) {
//			        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c2"),
//			            CompareOp.EQUAL, Bytes.toBytes(status)));
//			        list.addRange(new Range(Bytes.toBytes("f:c2"), Bytes.toBytes(status), CompareOp.EQUAL,
//			          null, CompareOp.NO_OP));
//			      }
//		      
//		      if (date != null) {
//			        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c4"),
//			            CompareOp.LESS, Bytes.toBytes(date)));
//			        list.addRange(new Range(Bytes.toBytes("f:c4"), null, CompareOp.NO_OP,
//			        		Bytes.toBytes(date), CompareOp.LESS));
//			      }
		      
		      
		      
		      if (c3_low > 0 && c3_top>0) {
		        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
		            CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(c3_low)));
		        
		        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
			            CompareOp.LESS_OR_EQUAL, Bytes.toBytes(c3_top)));
		        list.addRange(new Range(Bytes.toBytes("f:c3"), Bytes.toBytes(c3_low), CompareOp.GREATER_OR_EQUAL,
		          Bytes.toBytes(c3_top), CompareOp.LESS_OR_EQUAL));
		      } 
		      
		      
		      
		      if (useIndex) {
		    	scan.setFilter(filters);
		        scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Writables.getBytes(list));
	//		        scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes(true));
		      }
		      scan.setCacheBlocks(false);
		      scan.setCaching(1000);
		      /***************************************************/
			scan.addFamily(Bytes.toBytes("f"));
			
			ResultScanner rs=table.getScanner(scan);
			double sumResult=0;
//			for(Result r:rs){
//				System.out.println("rowkey:"+Bytes.toString(r.getRow()));
//				count++;
//				for(Cell cell:r.rawCells()){
//					if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c1")){
//						System.out.println("c1:"+Bytes.toInt(CellUtil.cloneValue(cell)));
//					}else  if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c2")){
//						System.out.println("c2:"+Bytes.toString(CellUtil.cloneValue(cell)));
//					}else if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c3")){
//						System.out.println("c3:"+Bytes.toDouble(CellUtil.cloneValue(cell)));
//					}else if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c4")){
//						System.out.println("c4:"+Bytes.toString(CellUtil.cloneValue(cell)));
//					}else if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c5")){
//						System.out.println("c5:"+Bytes.toString(CellUtil.cloneValue(cell)));
//					}else if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c6")){
//						System.out.println("c6:"+Bytes.toString(CellUtil.cloneValue(cell)));
//					}else if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c7")){
//						System.out.println("c7:"+Bytes.toInt(CellUtil.cloneValue(cell)));
//					}else if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("c8")){
//						System.out.println("c8:"+Bytes.toString(CellUtil.cloneValue(cell)));
//					}
//				}
//			}
//			System.out.println("count:"+count);
			
			long end=System.currentTimeMillis();
//			System.out.println("total micro seconds::"+(end-start) +" ms");
			/**
			 * this is the aggregate test
			 */
			AggregationClient client=new AggregationClient(conf);
			long startTime = System.currentTimeMillis();
			double sum=client.sum(table, new DoubleColumnInterpreter(), scan);
			long stopTime = System.currentTimeMillis();
			System.out.println("Time elapsed: " + (stopTime - startTime) + " ms" );
			System.out.println("total sum:"+sum);
			client.close();
			
	}
}
