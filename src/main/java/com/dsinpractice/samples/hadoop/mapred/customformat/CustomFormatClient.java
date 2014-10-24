package com.dsinpractice.samples.hadoop.mapred.customformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CustomFormatClient {


	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		/*if (args-Djava.security.krb5.realm=OX.AC.UK -Djava.security.krb5.kdc=kdc0.ox.ac.uk:kdc1.ox.ac.uk.length != 2) {
		      System.err.println("Usage: <input path> <output path>");
		      System.exit(-1);
		}*/
		
		Configuration conf = new Configuration();


		Job job = new Job(conf,"Custom Input Type");
		job.setJarByClass(CustomFormatClient.class);
		job.setNumReduceTasks(0);
		
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(MyKey.class);
		job.setOutputValueClass(MyValue.class);
		job.setInputFormatClass(MyInputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
	    
	    job.waitForCompletion(true);
	}


}
