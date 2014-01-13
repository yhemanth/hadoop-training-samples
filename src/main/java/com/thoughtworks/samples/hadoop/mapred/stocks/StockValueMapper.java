package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockValueMapper extends Mapper<Object,Text,Text,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String nasdaq_stock_line = value.toString();
        String[] fields = nasdaq_stock_line.split(",");
        context.write(new Text(fields[1]), new Text(nasdaq_stock_line));
    }
}
