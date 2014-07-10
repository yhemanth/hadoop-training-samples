package com.thoughtworks.samples.hadoop.mapred.avro.colorcount;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AvroJobInitializer {
  static Job setupAvroSerialization() throws IOException {
    Job job = new Job();
    String[] serializations = job.getConfiguration().getStrings("io.serializations");
    String[] newSerializations = new String[serializations.length + 1];
    System.arraycopy(serializations, 0, newSerializations, 0, serializations.length);
    newSerializations[newSerializations.length-1] = AvroSerialization.class.getName();
    job.getConfiguration().setStrings("io.serializations", newSerializations);
    return job;
  }
}
