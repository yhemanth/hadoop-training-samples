package com.dsinpractice.samples.hadoop.mapred.avro.colorcount;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ColorCountReducer extends Reducer<Text, IntWritable, AvroKey<String>, AvroValue<Integer>> {

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }

    context.write(new AvroKey<String>(key.toString()), new AvroValue<Integer>(sum));
  }
}
