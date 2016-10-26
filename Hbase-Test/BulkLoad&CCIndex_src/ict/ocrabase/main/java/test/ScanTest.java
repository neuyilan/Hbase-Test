package ict.ocrabase.main.java.test;

import ict.ocrabase.main.java.client.index.IndexAdmin;
import ict.ocrabase.main.java.client.index.IndexExistedException;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;
import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanTest {
	private static final byte[] tableName = Bytes.toBytes("t100M");
	  private static final byte[] newTableName = Bytes.toBytes("New");
	  private static Configuration conf;
	  private static IndexAdmin indexadmin;
	  private static IndexTableDescriptor indexDesc;
	  private static HBaseAdmin admin;
	  private static IndexSpecification[] index;
	  private static HTableDescriptor desc;
	  private static HTableDescriptor newdesc;
	  private static IndexTableDescriptor newIndexDesc;

	  public static void main(String[] args) throws IOException, IndexExistedException {
	    
		conf = HBaseConfiguration.create();
	    
	    admin = new HBaseAdmin(conf);
	    
	    int startkey = 20000;
	    int stopkey = 20990;
	    
	    HTable table =new HTable(conf, "t1-f1_c1");
	    long starttime = System.currentTimeMillis();
	    Scan scan = new Scan(Bytes.toBytes("20020"),Bytes.toBytes("20990"));
	    ResultScanner scanner = table.getScanner(scan);
	    int i=0;
	    for (;;i++){
	    	Result res =scanner.next();
	    	if(res==null){
	    		
	    		//System.out.println("null");
	    		break;
	    	}
	    	if(i%100000==0){
	    		//System.out.println(res.getRow());
	    		//System.out.println(res.getValue(Bytes.toBytes("f3"), Bytes.toBytes("c4")));
	    		System.out.println(i+" Time: "+(System.currentTimeMillis()-starttime));
	    	}
	    }
	    System.out.println(i+" Time: "+(System.currentTimeMillis()-starttime));
	  }
}
