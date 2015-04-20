package com.dsinpractice.samples.hadoop.mapred.charcount;

import java.io.IOException;
import java.util.Iterator;

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

public class CharCount {
	

	

	//Write client which will run the job
	public static void main(String[] args) {
		
		//1. create job conf
		JobConf conf = new JobConf(CharCount.class);
		conf.setJobName("CharCount");
		
		//2. set the output key and value type
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		//3. set the map and reduce class
		conf.setMapperClass(CharCountMap.class);
		conf.setReducerClass(CharCountReduce.class);
		
		//4. set the inoput output data format
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		//5. set the input / output directories
		//FileInputFormat.setInputPaths(conf, new Path("/hdfspath/data/wcinput/"));
		//FileOutputFormat.setOutputPath(conf, new Path("/hdfspath/data/ccoutput"));

        //5. set the input and output file path
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //6. run the job
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
