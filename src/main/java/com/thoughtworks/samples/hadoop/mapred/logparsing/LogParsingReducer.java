package com.thoughtworks.samples.hadoop.mapred.logparsing;

import com.thoughtworks.samples.hadoop.domain.HttpRequest;
import com.thoughtworks.samples.hadoop.domain.LogEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogParsingReducer extends Reducer<HttpRequest, LogEntry, HttpRequest, Text> {
    @Override
    public void reduce(HttpRequest key, Iterable<LogEntry> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        for (LogEntry entry : values) {
            stringBuilder.append(",");
            stringBuilder.append(String.format("%s %s", entry.getDate(), entry.getTime()));
        }
        context.write(key, new Text(stringBuilder.toString().substring(1)));
    }
}
