package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;

public class StockCompanyValueJoinReducerTest {
    @Test
    public void should_emit_record_with_company_name() {
        StockCompanyValueJoinReducer stockCompanyValueJoinReducer = new StockCompanyValueJoinReducer();
        ReduceDriver<Text, Text, Text, Text> reduceDriver =
                ReduceDriver.newReduceDriver(stockCompanyValueJoinReducer);
        ArrayList<Text> stockValuesAndCompanyName = new ArrayList<Text>();
        stockValuesAndCompanyName.add(new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"));
        stockValuesAndCompanyName.add(new Text("Carolina Bank Holdings Inc."));
        stockValuesAndCompanyName.add(new Text("NASDAQ,CLBH,2010-01-27,4.10,4.10,3.80,3.97,5100,3.97"));
        reduceDriver.withInput(new Text("CLBH"), stockValuesAndCompanyName);
        reduceDriver.withOutput(new Text("Carolina Bank Holdings Inc."),
                new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"));
        reduceDriver.withOutput(new Text("Carolina Bank Holdings Inc."),
                new Text("NASDAQ,CLBH,2010-01-27,4.10,4.10,3.80,3.97,5100,3.97"));

        reduceDriver.runTest();
    }

    @Test
    public void should_emit_unknown_as_company_name_if_no_mapping_found() {
        StockCompanyValueJoinReducer stockCompanyValueJoinReducer = new StockCompanyValueJoinReducer();
        ReduceDriver<Text, Text, Text, Text> reduceDriver =
                ReduceDriver.newReduceDriver(stockCompanyValueJoinReducer);
        ArrayList<Text> stockValuesAndCompanyName = new ArrayList<Text>();
        stockValuesAndCompanyName.add(new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"));
        stockValuesAndCompanyName.add(new Text("NASDAQ,CLBH,2010-01-27,4.10,4.10,3.80,3.97,5100,3.97"));
        reduceDriver.withInput(new Text("CLBH"), stockValuesAndCompanyName);
        reduceDriver.withOutput(new Text("Unknown"),
                new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"));
        reduceDriver.withOutput(new Text("Unknown"),
                new Text("NASDAQ,CLBH,2010-01-27,4.10,4.10,3.80,3.97,5100,3.97"));

        reduceDriver.runTest();

    }
}
