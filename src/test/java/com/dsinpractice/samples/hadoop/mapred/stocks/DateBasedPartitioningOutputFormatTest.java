package com.dsinpractice.samples.hadoop.mapred.stocks;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class DateBasedPartitioningOutputFormatTest {

    @Test
    public void should_return_output_directory_with_year_and_month_components() {
        DateBasedPartitioningOutputFormat dateBasedPartitioningOutputFormat = new DateBasedPartitioningOutputFormat();
        String fileName = dateBasedPartitioningOutputFormat.generateFileNameForKeyValue(
                new Text("2010-02-08"), new Text("NASDAQ,CLBH,2010-02-08,4.00,4.08,4.00,4.03,3100,4.03"), "part-00000");
        assertEquals("2010/02/part-00000", fileName);
    }

}
