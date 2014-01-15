package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class DateExtractionMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object position, Text text,
                    OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String record = text.toString();
        String[] fields = record.split(",");
        collector.collect(new Text(fields[2]), text);
    }
}
