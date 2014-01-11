package com.thoughtworks.samples.hadoop.mapred.logparsing;

import com.thoughtworks.samples.hadoop.domain.HttpRequest;
import com.thoughtworks.samples.hadoop.domain.LogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogParsingJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job logParsingJob = new Job(conf, "Log Parsing Job");
        logParsingJob.setJarByClass(LogParsingJob.class);

        logParsingJob.setMapperClass(LogParsingMapper.class);
        FileInputFormat.setInputPaths(logParsingJob, args[0]);
        logParsingJob.setMapOutputKeyClass(HttpRequest.class);
        logParsingJob.setMapOutputValueClass(LogEntry.class);

        logParsingJob.setNumReduceTasks(2);
        logParsingJob.setReducerClass(LogParsingReducer.class);
        logParsingJob.setOutputKeyClass(HttpRequest.class);
        logParsingJob.setOutputValueClass(Text.class);
        logParsingJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(logParsingJob, new Path(args[1]));

        logParsingJob.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new LogParsingJob(), args);
    }
}
