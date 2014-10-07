package com.dsinpractice.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class StockCompanyValueJoinReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Collection<String> stockValues = new ArrayList<String>();
        String companyName = "Unknown";
        for (Text value : values) {
            if (value.toString().startsWith("NASDAQ,")) {
                stockValues.add(value.toString());
            } else {
                companyName = value.toString();
            }
        }
        for (String stockValue : stockValues) {
            context.write(new Text(companyName), new Text(stockValue));
        }
    }
}
