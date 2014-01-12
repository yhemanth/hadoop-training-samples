package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class StockValueMapperTest {

    @Test
    public void should_map_stock_symbol_as_key_and_line_as_value() {
        StockValueMapper stockValueMapper = new StockValueMapper();
        MapDriver<Object,Text, Text, Text> mapDriver = MapDriver.newMapDriver(stockValueMapper);
        mapDriver.withInput(new LongWritable(0L), new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"));
        mapDriver.withOutput(new Text("CLBH"), new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"));
        mapDriver.runTest();
    }
}
