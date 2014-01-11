package com.thoughtworks.samples.hadoop.mapred.merge;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IdentityMapper extends Mapper<Object, Object, Object, Object> {
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}