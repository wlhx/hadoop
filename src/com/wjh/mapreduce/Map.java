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
		//������Ĵ��ı��ļ�������ת����String
		String line = value.toString();
		//��ӡһ�����������
		System.out.println(line);
		//�����ݰ��зָ�
		StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
		//�ֱ��ÿһ�п�ʼ����
		while (tokenizerArticle.hasMoreTokens()) {
			//ÿ�а��ո�ʼ����
			StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
			//ѧ����������
			String strName = tokenizerLine.nextToken();
			//ѧ���ɼ�����
			String strScore = tokenizerLine.nextToken();
			//ת����ʽ
			Text name = new Text(strName);
			int scoreInt = Integer.parseInt(strScore);
			//�����reduce
			context.write(name, new IntWritable(scoreInt));
			
		}
	}

}
