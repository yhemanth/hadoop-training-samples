package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class StockCompanyMapperTest {

    @Test
    public void should_map_stock_symbol_as_key_and_name_as_value() {
        StockCompanyMapper stockCompanyMapper = new StockCompanyMapper();
        MapDriver<Object,Text, Text, Text> stockCompanyMapperDriver = MapDriver.newMapDriver(stockCompanyMapper);
        stockCompanyMapperDriver.withInput(new LongWritable(0L), new Text("CLBH,Carolina Bank Holdings Inc."));
        stockCompanyMapperDriver.withOutput(new Text("CLBH"), new Text("Carolina Bank Holdings Inc."));

        stockCompanyMapperDriver.runTest();
    }
}
