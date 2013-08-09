package com.thoughtworks.samples.hadoop.mapred;

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

public class StragglerJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job stragglerJob = new Job(conf, "StragglerJob");
        stragglerJob.setJarByClass(StragglerJob.class);

        FileInputFormat.addInputPath(stragglerJob, new Path(args[0]));
        stragglerJob.setMapperClass(StragglerMapper.class);
        stragglerJob.setSpeculativeExecution(true);

        FileOutputFormat.setOutputPath(stragglerJob, new Path(args[1]));
        stragglerJob.setReducerClass(WCReducer.class);
        stragglerJob.setOutputKeyClass(Text.class);
        stragglerJob.setOutputValueClass(IntWritable.class);
        stragglerJob.setNumReduceTasks(2);

        stragglerJob.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StragglerJob(), args);
    }
}
