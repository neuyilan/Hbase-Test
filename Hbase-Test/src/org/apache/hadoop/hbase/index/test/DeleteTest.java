package org.apache.hadoop.hbase.index.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteTest {
	public DeleteTest() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		String tableName="orders_withindex";
		 HTable table = new HTable(conf, tableName);
		 String row="96000008";
		 Delete delete = new Delete(Bytes.toBytes(row));
		 delete.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
//		 delete.addColumns(Bytes.toBytes("f"), Bytes.toBytes("f3"),Integer.MAX_VALUE);
		 table.delete(delete);
		 table.close();
//		 admin.deleteColumn(tableName, "f");
	}
	
	public static void main(String args[]) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		new DeleteTest();
	}
}
