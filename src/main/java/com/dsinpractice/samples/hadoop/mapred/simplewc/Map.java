package com.dsinpractice.samples.hadoop.mapred.simplewc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by admin on 10/23/14.
 */
//Write a map class
public   class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            if(token.endsWith(".") || token.endsWith(",")){
                token = token.substring(0,token.length() - 1);
            }
            value.set(token.toLowerCase());
            output.collect(value, new IntWritable(1));
        }
    }

}