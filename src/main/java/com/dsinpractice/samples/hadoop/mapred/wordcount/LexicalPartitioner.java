package com.dsinpractice.samples.hadoop.mapred.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LexicalPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text word, IntWritable count, int numReducers) {
        String s = word.toString();
        if (s.length() == 0) {
            return 0;
        }
        char c = s.charAt(0);

        int partition = new Character(c).hashCode() % numReducers;
        return partition;
    }
}
