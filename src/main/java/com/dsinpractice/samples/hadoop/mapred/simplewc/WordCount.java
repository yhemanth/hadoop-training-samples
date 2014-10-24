package com.dsinpractice.samples.hadoop.mapred.simplewc;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

	
	// Write a client which does map reduce
	public static void main(String[] args) {

		//1. Create Job Config
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("WordCount");
		
		// [Optional] if you want to set number of reducer job you want to run
		conf.setNumReduceTasks(3);
		
		//3. set the map class and reduce class
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setPartitionerClass(Partition.class);

		//2. Set output key and value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		//4. Set the type of input and output formate. In this case it will be text input and text output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		//5. set the input and output file path
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //For the hardcorded paths use the following and comment the previous lines
		//FileInputFormat.setInputPaths(conf, new Path("/path/wcinput/"));
		//FileOutputFormat.setOutputPath(conf, new Path("/path/wcoutput/"));
		
		//6. run the job
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
