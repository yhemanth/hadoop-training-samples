package com.thoughtworks.samples.hadoop.mapred.failures;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StragglerMapper extends Mapper<Object, Text, Text, IntWritable> {

    public static final int MAP_DELAY_TIME = 10;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        straggleTask(context.getConfiguration());

        String[] words = value.toString().split(" ");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }

    private void straggleTask(Configuration configuration) throws InterruptedException {
        if (shouldBecomeStraggler(configuration)) {
            straggleFor(MAP_DELAY_TIME);
        }
    }

    private void straggleFor(int timeInMillis) throws InterruptedException {
        Thread.sleep(timeInMillis);
    }

    private boolean shouldBecomeStraggler(Configuration configuration) {
        TaskInfo taskInfo = getTaskInfo(configuration);
        return taskInfo.shouldBecomeStraggler();
    }

    private TaskInfo getTaskInfo(Configuration configuration) {
        String taskAttemptId = configuration.get("mapred.task.id");
        String[] attemptIdParts = taskAttemptId.split("_");
        return new TaskInfo(Integer.parseInt(attemptIdParts[attemptIdParts.length - 2]),
                Integer.parseInt(attemptIdParts[attemptIdParts.length - 1]));

    }

    private class TaskInfo {
        private final int taskId;
        private final int taskAttemptId;

        public TaskInfo(int taskId, int taskAttemptId) {
            this.taskId = taskId;
            this.taskAttemptId = taskAttemptId;
        }

        public boolean shouldBecomeStraggler() {
            return (isFirstAttempt() && (taskId % 2 == 0));
        }

        private boolean isFirstAttempt() {
            return (taskAttemptId == 0);
        }
    }
}
