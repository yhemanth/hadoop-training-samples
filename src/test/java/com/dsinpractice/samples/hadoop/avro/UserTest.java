package com.dsinpractice.samples.hadoop.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class UserTest {

  @Test
  public void should_read_a_serialized_avro_record() throws IOException {

    User user = new User("Hemanth", new Integer(0), "RED");

    File file = new File("usertest.avro");
    DatumWriter<User> userSerializer = new SpecificDatumWriter<User>(User.class);
    DataFileWriter<User> userDataFileWriter = new DataFileWriter<User>(userSerializer);
    userDataFileWriter.create(user.getSchema(), file);
    userDataFileWriter.append(user);
    userDataFileWriter.close();

    DatumReader<User> userDeserializer = new SpecificDatumReader<User>(User.class);
    DataFileReader<User> users = new DataFileReader<User>(file, userDeserializer);
    User expectedUser = null;
    while (users.hasNext()) {
      User nextUser = users.next(expectedUser);
      System.out.println("Read user: " + nextUser);
      assertEquals(user, nextUser);
    }
    users.close();
    file.delete();
  }

  @Test
  public void should_read_a_generic_serialized_avro_record() throws IOException {

    String avroSchemaDir = System.getProperty("avroSchemaDir");
    String userSchemaFile = avroSchemaDir + "/user.avsc";

    Schema userSchema = new Schema.Parser().parse(new File(userSchemaFile));

    GenericData.Record user = new GenericData.Record(userSchema);
    user.put("name", "Mihir");
    user.put("favorite_number", new Integer(7));
    user.put("favorite_color", "BLUE");

    File file = new File("usertest.avro");
    GenericDatumWriter<GenericRecord> userSerializer = new GenericDatumWriter<GenericRecord>(userSchema);
    DataFileWriter<GenericRecord> recordWriter = new DataFileWriter<GenericRecord>(userSerializer);
    recordWriter.create(userSchema, file);
    recordWriter.append(user);
    recordWriter.close();

    GenericDatumReader<GenericRecord> userDeserializer = new GenericDatumReader<GenericRecord>(userSchema);
    DataFileReader<GenericRecord> users = new DataFileReader<GenericRecord>(file, userDeserializer);
    User expectedUser = new User();
    while (users.hasNext()) {
      GenericRecord nextUser = users.next(expectedUser);
      System.out.println("Next record: " + nextUser);
      assertEquals(user, nextUser);
    }

    users.close();
    file.delete();
  }
}
