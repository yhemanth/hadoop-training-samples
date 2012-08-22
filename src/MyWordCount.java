import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyWordCount {

    public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text text, Context context)
                throws IOException, InterruptedException {
            String line = text.toString();
            String[] words = line.split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int wordCount = 0;
            for (IntWritable count : counts) {
                wordCount += count.get();
            }
            context.write(word, new IntWritable(wordCount));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job wordCountJob = new Job(conf, "MyWordCount");
        wordCountJob.setJarByClass(MyWordCount.class);
        wordCountJob.setMapperClass(WCMapper.class);
        wordCountJob.setReducerClass(WCReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        wordCountJob.waitForCompletion(true);
    }
}
