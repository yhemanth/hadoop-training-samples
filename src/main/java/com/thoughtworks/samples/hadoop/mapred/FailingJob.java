package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FailingJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = new Job(configuration);
        job.setJobName("FailingJob");
        job.setJarByClass(FailingJob.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setMapperClass(FailingMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.waitForCompletion(false);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new FailingJob(), args);
    }
}
