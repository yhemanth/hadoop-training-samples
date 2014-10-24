package com.dsinpractice.samples.hadoop.mapred.simplewc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * Created by admin on 10/23/14.
 */
public   class Partition implements org.apache.hadoop.mapred.Partitioner<Text, IntWritable>{

    @Override
    public void configure(JobConf arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getPartition(Text key, IntWritable value, int numOfPartition) {
        String keyVal = key.toString().toLowerCase();
        if(keyVal.equals("papa")){
            // if "papa", the put it to partition 1.
            return 0;
        } else if (keyVal.equals("yes") || keyVal.equals("no")){
            // if "no" or "yes", the put it to partition 2.
            return 1;
        } else {
            // For rest of the cases, put it to partition 0.
            return 2;
        }
    }

}
