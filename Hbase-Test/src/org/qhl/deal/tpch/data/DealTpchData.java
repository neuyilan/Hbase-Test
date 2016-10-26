package org.qhl.deal.tpch.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class DealTpchData {
	public static void main(String[] args)throws IOException {
		
		int count=0;
//		
		FileInputStream fis=new FileInputStream("/opt/tpch-test-data/large/backup1/xaa");
		InputStreamReader isr=new InputStreamReader(fis,"UTF-8");
		BufferedReader br =new BufferedReader(isr);
		String line="";
		
		FileOutputStream fos=new FileOutputStream("/opt/tpch-test-data/large/backup1/2000wanreal.txt");
		OutputStreamWriter osw=new OutputStreamWriter(fos,"UTF-8");
		BufferedWriter bw=new BufferedWriter(osw);
//		
		int length=0;
		while(((line=br.readLine())!=null)&&count<20000000){
			line=line.replace('|', ';');
			if(line.length()>95){
				length=95;
			}else{
				length=line.length();
			}
			line=line.substring(0, length);
			bw.write((count+100000000)+";"+line+"\n");
			count++;
		}
		bw.write("120000009;1;1;1;1;1;1;1;1;1;");
		br.close();
		isr.close();
		fis.close();
		
//		FileOutputStream fos=new FileOutputStream("/opt/tpch-test-data/orders.sql");
//		OutputStreamWriter osw=new OutputStreamWriter(fos,"UTF-8");
//		BufferedWriter bw=new BufferedWriter(osw);
//		
//		while(count<20000000){
//			count++;
//			String line="insert into TT( O_CUSTKEY, O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY, O_COMMENT ) VALUES (9469210,'O',76957.08,'1997-03-02','5-LOW','Clerk000057961',0,'deposits_haggle_blithely_blithely_special_deposits');";
//			bw.write(line);
//		}
		bw.close();
		osw.close();
		fos.close();
		
	}
	
//	public void readFile() throws IOException{
//		FileInputStream fis=new FileInputStream("/opt/tpch-test-data/large/xaa");
//		InputStreamReader isr=new InputStreamReader(fis,"UTF-8");
//		BufferedReader br =new BufferedReader(isr);
//		
//		String line="";
//		while((line=br.readLine())!=null){
//			writeFile(line);
//		}
//		
//		br.close();
//		isr.close();
//		fis.close();
//	}
//	
//	public void writeFile(String line) throws IOException{
//		FileOutputStream fos=new FileOutputStream("/opt/tpch-test-data/large/xaa-new");
//		OutputStreamWriter osw=new OutputStreamWriter(fos,"UTF-8");
//		BufferedWriter bw=new BufferedWriter(osw);
//
//		bw.write(line+"\n");
//		bw.close();
//		osw.close();
//		fos.close();
//	}
}
