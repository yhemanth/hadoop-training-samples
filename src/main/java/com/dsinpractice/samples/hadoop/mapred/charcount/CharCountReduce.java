package com.dsinpractice.samples.hadoop.mapred.charcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by admin on 10/23/14.
 */
// write Map class
//Write Reduce class
public   class CharCountReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        // Logic to collect and segregate the char counts per char

        int sum = 0;

        while(value.hasNext()){
            sum = sum + value.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }

}
