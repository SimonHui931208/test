package org.bigdata.day05;

import org.apache.hadoop.io.Text;


public class Test03 {

	public static void main(String [] args) throws Exception{
		Text text = new Text("Hello World");
		System.out.println(text.charAt(0));
	}
}
