package com.thoughtworks.samples.hadoop.mapred.failures;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FailingMapper extends Mapper<Object, Object, Object, Object> {

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        throw new RuntimeException("Failing map task");
    }
}
