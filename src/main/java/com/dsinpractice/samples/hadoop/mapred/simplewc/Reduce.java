package com.dsinpractice.samples.hadoop.mapred.simplewc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by admin on 10/23/14.
 */


// Write a Reduce class
public   class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        int sum = 0;
        while(value.hasNext()){
            System.out.println(value.next());
            sum ++;
        }

        output.collect(key, new IntWritable(sum));
    }

}

