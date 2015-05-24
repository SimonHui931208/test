/*
 * Hadoop工具类
 * 2015.5.14
 * SimonHui
 * */
package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {
	//创建文件夹
	public static void mkdir(String dirPath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.mkdirs(new Path(dirPath));
		fs.close();
	}
	
	// 创建文件
	public static void createFile(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.create(new Path(filePath));
		fs.close();
	}
	
	//删除文件
	public static void deleteFile(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		//fs.delete(new Path(filePath),false);//设置为true，只能删目录，false既可以删除目录，也可以删除文件
		fs.deleteOnExit(new Path(filePath));
		fs.close();
	}
	
	//遍历文件
	public static void listFile(String path) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		
		FileStatus[] status = fs.listStatus(new Path(path));
		
		for (FileStatus s : status){
			System.out.println(s.getPath().toString());
		}
		fs.close();
	}
	
	//上传文件
	public static void upload(String src, String dest) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		fs.close();
	}
	
	//下载文件
	public static void download(String src, String dest) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		
		fs.copyToLocalFile(new Path(src), new Path(dest));
		fs.close();
	}
	
	public static void main(String[] args) throws Exception{
		//mkdir("/a/b");
		//createFile("/Hello.java");
		//deleteFile("/Hello.java");
		//listFile("/");
		//upload("C:/Users/SimonHui/Desktop/login.jsp", "/hello.jsp");
		//download("/hello.jsp", "C:/Users/SimonHui/Desktop/login1.jsp");
	}
}
