package com.dsinpractice.samples.hadoop.mapred.avro.colorcount;

import com.dsinpractice.samples.hadoop.avro.User;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ColorCountMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {
  @Override
  protected void map(AvroKey<User> key, NullWritable value, Context context) throws IOException, InterruptedException {
    CharSequence favoriteColor = key.datum().getFavoriteColor();
    context.write(new Text(favoriteColor.toString()), new IntWritable(1));
  }
}
