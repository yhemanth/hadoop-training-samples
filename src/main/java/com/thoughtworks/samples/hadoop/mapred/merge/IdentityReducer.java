package com.thoughtworks.samples.hadoop.mapred.merge;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IdentityReducer extends Reducer<Object, Object, Object, Object> {
    @Override
    protected void reduce(Object key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
        for (Object value : values) {
            context.write(key, value);
        }
    }
}
