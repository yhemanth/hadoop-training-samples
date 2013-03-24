package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;

public class WordCountMRUnitTest {

  @Test
  public void should_count_words_in_input() {

    WCMapper wcMapper = new WCMapper();
    WCReducer wcReducer = new WCReducer();
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver
      = MapReduceDriver.newMapReduceDriver(wcMapper, wcReducer);

    mapReduceDriver.withInput(new LongWritable(0), new Text("Word1 Word2 Word2 Word1"));
    mapReduceDriver.withOutput(new Text("Word1"), new IntWritable(2));
    mapReduceDriver.withOutput(new Text("Word2"), new IntWritable(2));

    mapReduceDriver.runTest();
  }

  @Test
  public void mapper_should_count_occurrence_of_each_word() {

    WCMapper wcMapper = new WCMapper();
    MapDriver<Object, Text, Text, IntWritable> mapDriver
            = MapDriver.newMapDriver(wcMapper);

    mapDriver.withInput(new LongWritable(0), new Text("Word1 Word2 Word2 Word1"));
    mapDriver.withOutput(new Text("Word1"), new IntWritable(1));
    mapDriver.withOutput(new Text("Word2"), new IntWritable(1));
    mapDriver.withOutput(new Text("Word2"), new IntWritable(1));
    mapDriver.withOutput(new Text("Word1"), new IntWritable(1));
  }

  @Test
  public void reducer_should_sum_up_counts_for_a_word() {

    WCReducer wcReducer = new WCReducer();
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver
            = ReduceDriver.newReduceDriver(wcReducer);

    ArrayList<IntWritable> counts = new ArrayList<IntWritable>();
    for (int i=0; i<3; i++) {
     counts.add(new IntWritable(1));
    }

    reduceDriver.withInput(new Text("Word1"), counts);
    reduceDriver.withOutput(new Text("Word1"), new IntWritable(3));
  }
}
