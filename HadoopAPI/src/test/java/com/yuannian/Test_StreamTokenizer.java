package com.yuannian;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Test_StreamTokenizer {

	public static void main(String[] args) {
		String str = "hadoop mapreduce hive hue";
		StringTokenizer token = new StringTokenizer(str);
		StringBuffer aa = new StringBuffer(str);
		System.out.println(aa.reverse().toString());
		
		while (token.hasMoreElements()) {
			Object object = (Object) token.nextElement();
			System.out.println(object.toString());
			System.out.println("%%%%%%%%%%");
			
		}
	}

}
