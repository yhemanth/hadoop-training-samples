package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class DateBasedPartitioningOutputFormat extends MultipleTextOutputFormat<Text, Text> {

    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        System.out.println("Key: " + key);
        String date = key.toString();
        String[] fields = date.split("-");
        return String.format("%s/%s/%s", fields[0], fields[1], name);
    }
}
