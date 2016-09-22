package com.hadoop.intellipaat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

//hdfs getconf -confKey dfs.permissions.enabled

/**
 * For access with different users you have to add
 * 
 * 
 * <property>
	   <name>dfs.permissions.enabled</name>
	   <value>false</value>
   </property>
   
   
   <property>
	  <name>fs.default.name</name>
	  <value>hdfs://10.20.58.127:54310</value>
	  <description>The name of the default file system.  A URI whose
	  scheme and authority determine the FileSystem implementation.  The
	  uri's scheme determines the config property (fs.SCHEME.impl) naming
	  the FileSystem implementation class.  The uri's authority is used to
	  determine the host, port, etc. for a filesystem.</description>
  </property>


 * @author raghunandangupta
 *
 */
public class UploadFileFromLocal {

	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		// 10.41.87.90:9000
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
		Path file = new Path("hdfs://localhost:54310/user/tests.txt");
		if (hdfs.exists(file)) {
			hdfs.delete(file, true);
		}
		OutputStream os = hdfs.create(file, new Progressable() {
			public void progress() {
				System.out.println("...bytes written: ");
			}
		});
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		br.write("Hello World");
		br.close();
		hdfs.close();
	}
}
