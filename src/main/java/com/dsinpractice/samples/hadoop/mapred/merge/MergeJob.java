package com.dsinpractice.samples.hadoop.mapred.merge;

import com.dsinpractice.samples.hadoop.mapred.generic.IdentityMapper;
import com.dsinpractice.samples.hadoop.mapred.generic.ValueOnlyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MergeJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(MergeJob.class);
        job.setMapperClass(IdentityMapper.class);
        job.setReducerClass(ValueOnlyReducer.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MergeJob(), args);
    }
}
