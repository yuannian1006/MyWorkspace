package com.yuannian;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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



public class WebPvUvMapRedduce {

	public static class ColumnFilterMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {		
		Text mapOutputkey = new Text();
		IntWritable mapOutputValue = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//TODO
			if(value == null){
				return;			
			}
			String  line = value.toString();
			String [] strs = line.split("\t");
			String provinceID = strs[23]; 
			
			//每行数据总长度小于30个字段，脏数据
			if(strs.length < 30){
				/**
				 * 定义一个计数器
				 * 组名称：error group
				 * 错误内容：columns is less than 30 
				 */
				context.getCounter(
						"error group",
						"columns is less than 30"
						).increment(1);
				return;
			}
			
			//url 为空，也是脏数据
			if(StringUtils.isBlank(strs[1])){
				context.getCounter(
						"error group",
						"url  is blank"
						).increment(1);
				
				return;
			}
			
			//provinceID 为空，不用统计
			if(StringUtils.isBlank(strs[23])){
				context.getCounter(
						"error group",
						"provinceID  is blank"
						).increment(1);
				return;
			}
			
			mapOutputkey.set(provinceID );
			context.write(mapOutputkey, mapOutputValue);			
		}

	}

	public static class ColumnFilterReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
	 IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			//定义求和的临时变量sum
			int sum = 0;
			
			//遍历求和
			for(IntWritable value : values){
				sum += value.get();
			}
			
			outputValue.set(sum);
			
			context.write(key, outputValue);
			

		}

	}

	/**
	 * Driver(环境，输入输出路径，并行)
	 * 
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		// 1.获取配置文件
		Configuration conf = new Configuration();

		// 2.创建job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());

		// 3.设置job的相关参数
		// input -> map ->reduce -> ouput
		// 3.1输入
		Path inPath = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inPath);

		// 3.2 map class  ->根据需要修改Map类，输入输出类型
		job.setMapperClass(ColumnFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 3.3reduce class ->根据需要修改Reduce类，输入输出类型
		job.setReducerClass(ColumnFilterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 3.4ouput
		Path outPath = new Path(args[1]);
		// 可能会出错
		FileSystem fs = FileSystem.get(conf); //提交集群跑则无所谓
		//FileSystem fs = outPath.getFileSystem(conf);//本地(windows)跑需要这种方式
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		FileOutputFormat.setOutputPath(job, outPath);

		// 4.提交job
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
//		args = new String[] {
//				"hdfs://hadoop.beifeng.com:8020/input/2015082818",
//				"hdfs://hadoop.beifeng.com:8020/output4"
//		};
		
	args = new String[] {
				"hdfs://com.yuannian04:8020/input/2015082818",
				"hdfs://com.yuannian04:8020/output"
		};

		// 运行方法
		int status = new WebPvUvMapRedduce().run(args);
		System.exit(status);
	}
}
