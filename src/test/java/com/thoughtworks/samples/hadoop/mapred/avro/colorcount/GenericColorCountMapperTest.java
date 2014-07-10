package com.thoughtworks.samples.hadoop.mapred.avro.colorcount;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class GenericColorCountMapperTest {

  @Test
  public void should_extract_color_with_count_one() throws IOException {

    String avroSchemaDir = System.getProperty("avroSchemaDir");
    String userSchemaFile = avroSchemaDir + "/user.avsc";
    Schema userSchema = new Schema.Parser().parse(new File(userSchemaFile));

    GenericData.Record user = new GenericData.Record(userSchema);
    user.put("name", "user1");
    user.put("favorite_color", "RED");
    user.put("favorite_number", new Integer(1));
    AvroKey<GenericRecord> key = new AvroKey<GenericRecord>(user);

    MapDriver mapDriver = MapDriver.newMapDriver(new GenericColorCountMapper());
    mapDriver.withInput(key, NullWritable.get());
    mapDriver.withOutput(new Text("RED"), new IntWritable(1));

    mapDriver.runTest();
  }
}
