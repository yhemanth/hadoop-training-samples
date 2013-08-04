package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TimingOutJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        configuration.setInt("mapred.task.timeout", 10000);

        Job job = new Job(configuration);
        job.setJobName("Timing Out Job");
        job.setJarByClass(TimingOutJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setMapperClass(MapperThatTimesOut.class);

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.waitForCompletion(false);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new TimingOutJob(), args);
    }
}
