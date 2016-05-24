package com.wjh.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
	public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()){
				//计算总分
				sum += iterator.next().get();
				//统计总的科目数
				count++;
			}
			int average = (int)sum/count;
			context.write(key, new IntWritable(average));
	}
}
