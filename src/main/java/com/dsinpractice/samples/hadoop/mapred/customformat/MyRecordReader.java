package com.dsinpractice.samples.hadoop.mapred.customformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader extends RecordReader<MyKey, MyValue> {

	private MyKey myKey;
	private MyValue myValue;
	private LineRecordReader reader = new LineRecordReader();

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		reader.close();
	}

	@Override
	public MyKey getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return myKey;
	}

	@Override
	public MyValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return myValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		reader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean isNextValue = reader.nextKeyValue();
		if (isNextValue) {
			if (myKey == null) {
				myKey = new MyKey();
			}
			if (myValue == null) {
				myValue = new MyValue();
			}
			Text line = reader.getCurrentValue();
			String[] tokens = line.toString().split(",");
			myKey.setImei(new Text(tokens[0]));
			myKey.setDeviceModel(new Text(tokens[1]));
			myValue.setStatus(new Text(tokens[2]));
			myValue.setCountry(new Text(tokens[3]));
			myValue.setBasePhone(new Text(tokens[4]));
			myValue.setAdvPhone(new Text(tokens[5]));

		} else {
			myKey = null;
			myValue = null;
		}
		return isNextValue;
	}

}
