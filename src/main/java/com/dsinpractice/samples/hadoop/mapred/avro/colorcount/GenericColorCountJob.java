package com.dsinpractice.samples.hadoop.mapred.avro.colorcount;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GenericColorCountJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Job job = new Job(getConf(), "generic-color-count-using-avro");
    job.setJarByClass(GenericColorCountJob.class);

    configureMapper(job, args);
    configureReducer(job, args);

    return job.waitForCompletion(true) ? 0 : -1;
  }

  private void configureReducer(Job job, String[] args) {
    job.setReducerClass(ColorCountReducer.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));
  }

  private void configureMapper(Job job, String[] args) throws IOException {
    job.setMapperClass(GenericColorCountMapper.class);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroKeyInputFormat.setInputPaths(job, args[0]);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new GenericColorCountJob(), args);
  }
}
