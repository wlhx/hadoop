package com.wjh.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Text, LongWritable,  Text, IntWritable>{
	
	@Override
	protected void map(Text key, LongWritable  value,
			Context context)
			throws IOException, InterruptedException {
		//将输入的纯文本文件的数据转化成String
		String line = value.toString();
		//打印一下输入的内容
		System.out.println(line);
		//将数据按行分隔
		StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
		//分别对每一行开始处理
		while (tokenizerArticle.hasMoreTokens()) {
			//每行按空格开始划分
			StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
			//学生姓名部分
			String strName = tokenizerLine.nextToken();
			//学生成绩部分
			String strScore = tokenizerLine.nextToken();
			//转换格式
			Text name = new Text(strName);
			int scoreInt = Integer.parseInt(strScore);
			//输出到reduce
			context.write(name, new IntWritable(scoreInt));
			
		}
	}

}
