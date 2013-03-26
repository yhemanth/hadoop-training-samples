package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WCReducerTest {

    @Test
    public void should_add_all_values_passed_for_a_word() throws IOException, InterruptedException {

        List<IntWritable> counts = new ArrayList<IntWritable>();
        counts.add(new IntWritable(1));
        counts.add(new IntWritable(2));
        Reducer.Context context = mock(Reducer.Context.class);

        WCReducer wcReducer = new WCReducer();
        wcReducer.reduce(new Text("Word1"), counts, context);

        verify(context, times(1)).write(new Text("Word1"), new IntWritable(3));
    }


    @Test
    public void reducer_should_sum_up_counts_for_a_word() {

        WCReducer wcReducer = new WCReducer();
        ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver
                = ReduceDriver.newReduceDriver(wcReducer);

        ArrayList<IntWritable> counts = new ArrayList<IntWritable>();
        for (int i = 0; i < 3; i++) {
            counts.add(new IntWritable(1));
        }

        reduceDriver.withInput(new Text("Word1"), counts);
        reduceDriver.withOutput(new Text("Word1"), new IntWritable(3));
    }
}
