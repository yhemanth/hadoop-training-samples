package com.thoughtworks.samples.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text text, Context context)
            throws IOException, InterruptedException {
        String line = text.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}