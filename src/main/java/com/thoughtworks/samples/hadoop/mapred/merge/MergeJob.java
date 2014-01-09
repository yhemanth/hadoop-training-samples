package com.thoughtworks.samples.hadoop.mapred.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MergeJob extends Configured implements Tool {

    private static class IdentityMapper extends Mapper<Object, Object, Object, Object> {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    private static class IdentityReducer extends Reducer<Object, Object, Object, Object> {
        @Override
        protected void reduce(Object key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
            for (Object value : values) {
                context.write(value, null);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(MergeJob.class);
        job.setMapperClass(IdentityMapper.class);
        job.setReducerClass(IdentityReducer.class);
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
