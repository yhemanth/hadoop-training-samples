package com.dsinpractice.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

public class DateExtractionMapperTest {

    @Test
    public void should_extract_date_from_record() {

        DateExtractionMapper dateExtractionMapper = new DateExtractionMapper();
        MapDriver<Object, Text, Text, Text> driver = MapDriver.newMapDriver(dateExtractionMapper);
        Text record = new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03");
        driver.withInput(new LongWritable(0L), record);
        driver.withOutput(new Text("2010-02-08"), record);
        driver.runTest();
    }
}
