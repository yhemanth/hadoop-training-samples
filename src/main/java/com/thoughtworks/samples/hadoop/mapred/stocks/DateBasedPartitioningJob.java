package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DateBasedPartitioningJob extends Configured implements Tool {

    public static final String JOB_BASE_PATH = "job.base.path";

    @Override
    public int run(String[] args) throws Exception {

        JobConf jobConf = new JobConf(getConf());
        jobConf.setJarByClass(DateBasedPartitioningJob.class);

        jobConf.setMapperClass(DateExtractionMapper.class);
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);

        jobConf.setReducerClass(IdentityReducer.class);
        jobConf.setNumReduceTasks(3);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);
        jobConf.setOutputFormat(DateBasedPartitioningOutputFormat.class);
        DateBasedPartitioningOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        RunningJob runningJob = JobClient.runJob(jobConf);
        runningJob.waitForCompletion();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DateBasedPartitioningJob(), args);
    }
}
