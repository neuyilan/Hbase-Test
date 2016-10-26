package ict.hbase.ycsb.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTableIndex {
	private String tableName = "user_middle";
	private boolean useIndex = true;

	public CreateTableIndex() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		System.out.println("((((((((((((((((((((");
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		if (useIndex) {
			IndexDescriptor index1 = new IndexDescriptor(
					Bytes.toBytes("field0"), DataType.STRING);
//			IndexDescriptor index2 = new IndexDescriptor(
//					Bytes.toBytes("field1"), DataType.STRING);
//			IndexDescriptor index3 = new IndexDescriptor(
//					Bytes.toBytes("field2"), DataType.STRING);

			IndexColumnDescriptor family = new IndexColumnDescriptor("f");
			family.addIndex(index1);
//			family.addIndex(index2);
//			family.addIndex(index3);

			tableDesc.addFamily(family);
			admin.createTable(tableDesc);
			admin.close();
		}

	}

	public static void main(String args[]) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		CreateTableIndex ct = new CreateTableIndex();
	}
}
