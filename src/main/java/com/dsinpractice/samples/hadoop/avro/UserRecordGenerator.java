package com.dsinpractice.samples.hadoop.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Random;

public class UserRecordGenerator extends Configured implements Tool {

  static final String[] FAVORITE_COLORS = new String[]{ "RED", "GREEN", "YELLOW", "BLACK", "WHITE", "BLUE", "PINK"};

  @Override
  public int run(String[] args) throws Exception {
    String avroSchemaDir = System.getProperty("avroSchemaDir");
    String userSchemaFile = avroSchemaDir + "/user.avsc";
    Schema userSchema = new Schema.Parser().parse(new File(userSchemaFile));

    int numUsers = Integer.parseInt(args[0]);
    Random favNumberRandomizer = new Random();
    Random favColorRandomizer = new Random();

    File outputFile = new File(args[1]);
    GenericDatumWriter<GenericRecord> userSerializer = new GenericDatumWriter<GenericRecord>(userSchema);
    DataFileWriter<GenericRecord> dataWriter = new DataFileWriter<GenericRecord>(userSerializer);
    dataWriter.create(userSchema, outputFile);

    for (int i=0; i<numUsers; i++) {
      GenericData.Record user = new GenericData.Record(userSchema);
      user.put("name", "User:"+i);
      user.put("favorite_number", new Integer(favNumberRandomizer.nextInt(numUsers+1)));
      user.put("favorite_color", FAVORITE_COLORS[favColorRandomizer.nextInt(numUsers+1)%FAVORITE_COLORS.length]);
      System.out.println(user);
      dataWriter.append(user);
    }

    dataWriter.close();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new UserRecordGenerator(), args);
  }


}
