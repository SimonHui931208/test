/*
 * Hadoop配置信息
 * 2015.5.10
 * SimonHui
 * Detail:运用单例模式
 * */
package org.bigdata.util;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfig {
	
	private HadoopConfig(){}
	
	private static Configuration config;
	public static Configuration getConfig(){
		if (config == null){
			config = new Configuration();
			config.addResource(HadoopConfig.class.getResource("core-site.xml"));
			config.addResource(HadoopConfig.class.getResource("hdfs-site.xml"));
			config.addResource(HadoopConfig.class.getResource("yarn-site.xml"));
		}
		return config;
	}
	
	
}
