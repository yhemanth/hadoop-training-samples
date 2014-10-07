package com.dsinpractice.samples.hadoop.mapred.logparsing;

import com.dsinpractice.samples.hadoop.domain.HttpRequest;
import com.dsinpractice.samples.hadoop.domain.LogEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogParsingMapper extends Mapper<Object, Text, HttpRequest, LogEntry> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String logLine = value.toString();
        String[] fields = logLine.split(" ");
        String httpMethod = fields[6];
        String url = fields[7];
        HttpRequest httpRequest = new HttpRequest(httpMethod, url);
        String date = fields[0].substring(1);
        String time = fields[1];
        context.write(httpRequest, new LogEntry(date, time, httpRequest));
    }
}
