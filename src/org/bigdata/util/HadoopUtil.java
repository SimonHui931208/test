/*
 * Hadoop������
 * 2015.5.14
 * SimonHui
 * */
package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {
	//�����ļ���
	public static void mkdir(String dirPath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.mkdirs(new Path(dirPath));
		fs.close();
	}
	
	// �����ļ�
	public static void createFile(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		fs.create(new Path(filePath));
		fs.close();
	}
	
	//ɾ���ļ�
	public static void deleteFile(String filePath) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		//fs.delete(new Path(filePath),false);//����Ϊtrue��ֻ��ɾĿ¼��false�ȿ���ɾ��Ŀ¼��Ҳ����ɾ���ļ�
		fs.deleteOnExit(new Path(filePath));
		fs.close();
	}
	
	//�����ļ�
	public static void listFile(String path) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		
		FileStatus[] status = fs.listStatus(new Path(path));
		
		for (FileStatus s : status){
			System.out.println(s.getPath().toString());
		}
		fs.close();
	}
	
	//�ϴ��ļ�
	public static void upload(String src, String dest) throws Exception{
		Configuration config = HadoopConfig.getConfig();
		FileSystem fs = FileSystem.get(config);
		
		fs.copyFromLocalFile(new Path(src), new Path(dest));
		fs.close();
	}
	
	//�����ļ�
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
