package org.hadoop.pract.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;


public class HbaseLoad {

	public static void main(String[] args) throws IOException {
		// Instantiating Configuration class
		Configuration conf = new Configuration();
		
		String nameNodeURL = "192.168.0.60";
		
		// general hbase setting
		conf.set("hbase.rootdir", "hdfs://" + nameNodeURL + ":" + 8020 +"/hbase");
		conf.setBoolean("hbase.cluster.distributed", true);
		conf.set("hbase.zookeeper.quorum", nameNodeURL);
		conf.setInt("hbase.client.scanner.caching", 10000);
		// ... some other settings
		
		 Configuration config = HBaseConfiguration.create(conf);

	      // Instantiating HTable class
	      HTable hTable = new HTable(config, "demo_table");
	      
	      ResultScanner resultScanner = hTable.getScanner(Bytes.toBytes("Name"));
	      System.out.println(resultScanner);
	      for (Iterator iterator = resultScanner.iterator(); iterator.hasNext();) {
			Result result = (Result) iterator.next();
			
		System.out.println(new String(result.value()));
	      
			
	
			
		}
	      

	      // Instantiating Put class
	      // accepts a row name.
	      /*Put p = new Put(Bytes.toBytes("200"));
	      p.add(Bytes.toBytes("coloumn family "), Bytes.toBytes("column name"),Bytes.toBytes("value"));
*/
	}


}
