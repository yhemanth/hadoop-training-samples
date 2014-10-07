package com.dsinpractice.samples.hadoop.mapred.avro.colorcount;


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenericColorCountMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, IntWritable> {
  @Override
  protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
    GenericRecord user = key.datum();
    String favorite_color = (String) user.get("favorite_color");
    context.write(new Text(favorite_color), new IntWritable(1));
  }
}
