package com.thoughtworks.samples.hadoop.mapred.logparsing;

import com.thoughtworks.samples.hadoop.domain.HttpRequest;
import com.thoughtworks.samples.hadoop.domain.LogEntry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class LogParsingMapperTest {

    @Test
    public void should_parse_log_line_into_map_record() {
        LogParsingMapper logParsingMapper = new LogParsingMapper();
        MapDriver<Object, Text, HttpRequest, LogEntry> mapDriver
                = MapDriver.newMapDriver(logParsingMapper);
        mapDriver.withInput(new LongWritable(0),
                new Text("[29/Apr/2013 10:35:31 +0000] INFO 10.218.9.7 - GET /debug/check_config_ajax"));
        HttpRequest httpRequest = new HttpRequest("GET", "/debug/check_config_ajax");
        mapDriver.withOutput(httpRequest,
                new LogEntry("29/Apr/2013", "10:35:31", httpRequest));
        mapDriver.runTest();
    }
}
