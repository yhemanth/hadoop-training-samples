package com.dsinpractice.samples.hadoop.mapred.charcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by admin on 10/23/14.
 */
// write Map class
public  class CharCountMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        // Logic to read line and count chars

        String line = value.toString();

        if (line == null || line.equals(""))
            return;

        for (int i = 0; i < line.length(); i++) {
            value.set("" + line.charAt(i));
            output.collect(value, new IntWritable(1));
        }

    }

}