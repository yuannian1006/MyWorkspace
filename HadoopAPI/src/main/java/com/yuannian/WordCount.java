package com.yuannian;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce
 * 
 * @author NianYuan
 * 假设对 以下内容统计单词
 * this is my first mapreduce JAVA API
 * today is sunny day
 * I'm so tried
 * com on , man
 *
 */

public class WordCount extends Configured implements Tool {
	//step 1:Map Class
	//map 输入的格式为 <0,this is my first mapreduce JAVA API>
	//<35,today is sunny day>

	public static class WordCountMaper extends Mapper<LongWritable, Text, Text, IntWritable>{
		//Mapper<IntWritable, Text, Text, IntWritable>
		//Mapper<输入key 类型, 输入value类型, 输出key 类型, 输出value类型>
		private Text mapOutKey = new Text();	//定义输出key
		private IntWritable mapOutPut = new IntWritable(1);	//定义输出value
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();	//把输入的值转换成字符串类型
			String[] str = line.split(" ");	//用空格切割出，字符串数组
			for(String word : str) {
				//把每行切割后的字符数组的每个元素封装成<key,value>形式，放入context对象中
				//<this,1>,<is,1>,<my,1>...
				mapOutKey.set(word);
				context.write(mapOutKey, mapOutPut);
			}

		}

	}

	//step 2:Reduce Class
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;	//初始化每个单词总次数

			for(IntWritable value : values){
				//统计每个单词出现的次数
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));	//统计Ok后输出
		}

	}

	//step 3:Driver ,compoment job
	public int run(String[] arg) throws Exception {
		//1.获取默认配置hdfs-dufault.xml core-default.xml yarn-defalut.xml
		Configuration conf = new Configuration();
		//2.根据需要设置配置信息
		//3.conf.set(name, value);
		//根据配置生成job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		//设置job中资源所在jar包
		job.setJarByClass(getClass());
		//4.设置job具体的数据目录 map逻辑  reducer逻辑输出目录


		//设置输入目录
		Path inputPath = new Path(arg[0]);
		FileInputFormat.setInputPaths(job, inputPath);
		//map阶段
		job.setMapperClass(WordCountMaper.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//reduce阶段
		//job.setNumReduceTasks(2);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//输出目录
		Path outputPath = new Path(arg[1]);
		FileSystem fs = outputPath.getFileSystem(conf);

		//设置输出如果存在则自动删除
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		FileOutputFormat.setOutputPath(job, outputPath);
		//提交job
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		args = new String[]{
				"hdfs://com.yuannian04:8020/input/b.txt",
				"hdfs://com.yuannian04:8020/output1"};
		
		//这个方法调用tool的run(String[])方法，并使用conf中的参数，以及args中的参数，而args一般来源于命令行。
		int status = ToolRunner.run(conf, new WordCount(), args);
		//status为0时退出整个虚拟机,整个虚拟机里的内容都停掉了,为正数时程序正常推出
		System.exit(status);

	}

}
