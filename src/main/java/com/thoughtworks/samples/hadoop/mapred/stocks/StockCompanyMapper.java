package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockCompanyMapper extends Mapper<Object,Text,Text,Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String companySymbolAndName = value.toString();
        String[] fields = companySymbolAndName.split(",", 2);
        context.write(new Text(fields[0]), new Text(fields[1]));
    }
}
