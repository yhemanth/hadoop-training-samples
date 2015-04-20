package com.dsinpractice.samples.hadoop.mapred.customformat;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class MyInputFormat extends FileInputFormat<MyKey, MyValue>{

	@Override
	public RecordReader<MyKey, MyValue> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new MyRecordReader();
	}

}
