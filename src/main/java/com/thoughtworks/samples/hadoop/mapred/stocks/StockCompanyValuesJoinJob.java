package com.thoughtworks.samples.hadoop.mapred.stocks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StockCompanyValuesJoinJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "Stock Company Values Join Job");
        job.setJarByClass(StockCompanyValuesJoinJob.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StockCompanyMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StockValueMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(StockCompanyValueJoinReducer.class);
        job.setNumReduceTasks(2);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new StockCompanyValuesJoinJob(), args);
    }
}
