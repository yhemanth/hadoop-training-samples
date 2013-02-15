package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class WCMapperTest {

    @Test
    public void should_count_each_word_in_line_as_one_occurrence() throws IOException, InterruptedException {
        WCMapper wcMapper = new WCMapper();
        LongWritable dummyKey = new LongWritable();
        Text inputValue = new Text("Word1 Word2 Word1");
        Mapper.Context context = mock(Mapper.Context.class);
        wcMapper.map(dummyKey, inputValue, context);

        verify(context, times(2)).write(new Text("Word1"), new IntWritable(1));
        verify(context, times(1)).write(new Text("Word2"), new IntWritable(1));
    }
}
