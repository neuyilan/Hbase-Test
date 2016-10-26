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

public class CreateTableNoIndex {
	private String tableName = "user";
	private boolean useIndex = true;

	public CreateTableNoIndex() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDesc = new HTableDescriptor(tableName);

		IndexColumnDescriptor family = new IndexColumnDescriptor("f");

		tableDesc.addFamily(family);
		admin.createTable(tableDesc);
		admin.close();

	}

	public static void main(String args[]) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		CreateTableNoIndex ct = new CreateTableNoIndex();
	}
}
