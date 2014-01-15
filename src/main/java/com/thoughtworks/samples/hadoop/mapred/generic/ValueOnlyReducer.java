package com.thoughtworks.samples.hadoop.mapred.generic;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValueOnlyReducer extends Reducer<Object, Object, Object, Object> {
    @Override
    public void reduce(Object key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
        for (Object value : values) {
            context.write(value, NullWritable.get());
        }
    }
}
