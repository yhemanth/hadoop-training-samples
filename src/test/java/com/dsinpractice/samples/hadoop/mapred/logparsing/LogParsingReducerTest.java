package com.dsinpractice.samples.hadoop.mapred.logparsing;

import com.dsinpractice.samples.hadoop.domain.HttpRequest;
import com.dsinpractice.samples.hadoop.domain.LogEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LogParsingReducerTest {

    @Test
    public void should_collect_request_times() {
        LogParsingReducer logParsingReducer = new LogParsingReducer();
        ReduceDriver<HttpRequest, LogEntry, HttpRequest, Text> logParsingReduceDriver
                = ReduceDriver.newReduceDriver(logParsingReducer);

        HttpRequest key = new HttpRequest("GET", "a");
        LogEntry logEntry1 = new LogEntry("29/Apr/2013", "12:00:00", key);
        LogEntry logEntry2 = new LogEntry("29/Apr/2013", "12:00:01", key);
        LogEntry logEntry3 = new LogEntry("30/Apr/2013", "12:00:01", key);
        List<LogEntry> logEntries = new ArrayList<LogEntry>();
        logEntries.add(logEntry1);
        logEntries.add(logEntry2);
        logEntries.add(logEntry3);

        logParsingReduceDriver.withInput(key, logEntries);
        logParsingReduceDriver.withOutput(key,
                new Text("29/Apr/2013 12:00:00,29/Apr/2013 12:00:01,30/Apr/2013 12:00:01"));

        logParsingReduceDriver.runTest();
    }
}
