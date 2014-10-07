package com.dsinpractice.samples.hadoop.mapred.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StopWordsFilterJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job stopWordsFilterJob = new Job(conf, "StopWordsFilter");
        stopWordsFilterJob.setJarByClass(StopWordsFilterJob.class);

        stopWordsFilterJob.setMapperClass(StopWordsFilterMapper.class);
        FileInputFormat.addInputPath(stopWordsFilterJob, new Path(args[0]));

        stopWordsFilterJob.setNumReduceTasks(2);
        stopWordsFilterJob.setReducerClass(WCReducer.class);
        stopWordsFilterJob.setOutputKeyClass(Text.class);
        stopWordsFilterJob.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(stopWordsFilterJob, new Path(args[1]));

        stopWordsFilterJob.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StopWordsFilterJob(), args);
    }
}
