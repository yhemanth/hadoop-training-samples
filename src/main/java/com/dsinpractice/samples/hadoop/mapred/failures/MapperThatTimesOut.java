package com.dsinpractice.samples.hadoop.mapred.failures;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperThatTimesOut extends Mapper<Object, Object, Object, Object> {

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int taskTimeout = configuration.getInt("mapred.task.timeout", 600000);
        int sleepInterval = taskTimeout * 2;
        System.out.println("Sleeping for " + sleepInterval + " milliseconds.");
        Thread.sleep(sleepInterval);
    }
}
