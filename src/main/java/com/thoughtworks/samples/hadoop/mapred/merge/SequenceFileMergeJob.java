package com.thoughtworks.samples.hadoop.mapred.merge;

import com.thoughtworks.samples.hadoop.mapred.generic.IdentityMapper;
import com.thoughtworks.samples.hadoop.mapred.generic.IdentityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SequenceFileMergeJob extends Configured implements Tool {

    private Class<?> keyClass;
    private Class<?> valueClass;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = setupJob(conf);
        keyClass = Class.forName(conf.get("sequencefilemergejob.keyclass"));
        valueClass = Class.forName(conf.get("sequencefilemergejob.valueclass"));
        configureMapTasks(args[0], job);
        configureReduceTasks(args[1], job);

        job.waitForCompletion(true);
        return 0;
    }

    private Job setupJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "Sequence File Merge Job");
        job.setJarByClass(SequenceFileMergeJob.class);
        return job;
    }

    private void configureReduceTasks(String arg, Job job) {
        job.setNumReduceTasks(1);
        job.setReducerClass(IdentityReducer.class);
        job.setOutputKeyClass(keyClass);
        job.setOutputValueClass(valueClass);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(arg));
    }

    private void configureMapTasks(String arg, Job job) throws IOException {
        job.setMapperClass(IdentityMapper.class);
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, arg);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new SequenceFileMergeJob(), args);
    }
}
