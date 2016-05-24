package com.wjh.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapReduce extends Configured implements Tool{


	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		//System.out.println(args[0]+"   "+args[1]);
		//获取配置信息
		Configuration configuration = new Configuration();
		//很重要的一句话
		configuration.set("mapred.job.tracker", "192.168.19.131:9001");
		//定义参数
		String[] ioArgs=new String[]{"hdfs://192.168.19.131:9000/input/","hdfs://192.168.19.131:9000/output2"};
		String[] othersArgs = new GenericOptionsParser(configuration,ioArgs).getRemainingArgs();
		
		if(othersArgs.length !=2){
			System.err.println("Usage: MapReduce <in> <out>");
			return 0;
		}
		//创建job，设置配置信息和job名称
		Job job = new Job(configuration);
		
		//关键工作，5部分
		//1.设置job运行的类是哪一个
		job.setJarByClass(MapReduce.class);
		
		//2.设置mapper和reduce类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//3.设置输入文件的目录  和输出文件的目录
		System.out.println(othersArgs[0]);
		FileInputFormat.setInputPaths(job, new Path(othersArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(othersArgs[1]));
		
		//4.设置输出结果的  key   value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//5.提交job，等待运行结果，并在客户端显示运行信息
		boolean isSuccess = job.waitForCompletion(true);
		//结束程序
		return isSuccess ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		/*if (args.length!=2) {
			return;
		}
		System.out.println(args[0]+"   "+args[1]);*/
		int ret = ToolRunner.run(new MapReduce(), args);
		System.exit(ret);
	}

}
