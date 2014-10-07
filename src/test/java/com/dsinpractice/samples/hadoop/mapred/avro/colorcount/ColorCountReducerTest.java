package com.dsinpractice.samples.hadoop.mapred.avro.colorcount;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class ColorCountReducerTest {

  @Test
  public void should_sum_all_counts_for_a_color() throws IOException {

    Job job = AvroJobInitializer.setupAvroSerialization();
    job.getConfiguration().setStrings("avro.serialization.key.writer.schema",
      Schema.create(Schema.Type.STRING).toString(true));
    job.getConfiguration().setStrings("avro.serialization.value.writer.schema",
      Schema.create(Schema.Type.INT).toString(true));

    ReduceDriver<Text, IntWritable, AvroKey<String>, AvroValue<Integer>> reduceDriver
      = ReduceDriver.newReduceDriver(new ColorCountReducer()).withConfiguration(job.getConfiguration());
    ArrayList<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("RED"), values);
    reduceDriver.withOutput(new AvroKey<String>("RED"), new AvroValue<Integer>(2));
    reduceDriver.runTest();

  }
}
