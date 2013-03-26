package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;

public class WordCountEndToEndTest {

    @Test
    public void should_count_words_in_input() {

        WCMapper wcMapper = new WCMapper();
        WCReducer wcReducer = new WCReducer();
        MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver
                = MapReduceDriver.newMapReduceDriver(wcMapper, wcReducer);

        mapReduceDriver.withInput(new LongWritable(0), new Text("word1 word2 Word2 Word1"));
        mapReduceDriver.withOutput(new Text("word1"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("word2"), new IntWritable(2));

        mapReduceDriver.runTest();
    }

}
