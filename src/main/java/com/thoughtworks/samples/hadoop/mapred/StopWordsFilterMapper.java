package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class StopWordsFilterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Set<String> stopWords = new HashSet<String>();
    private static Logger logger = Logger.getLogger(StopWordsFilterMapper.class.getName());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(configuration);
        Path stopWordsFile = localCacheFiles[0];
        logger.info("Loading stop words from file: " + stopWordsFile.getName());
        FSDataInputStream inputStream = FileSystem.getLocal(configuration).open(stopWordsFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String word = bufferedReader.readLine();
        while (word != null) {
            stopWords.add(word);
            word = bufferedReader.readLine();
        }
        bufferedReader.close();
        logger.info(String.format("Loaded %d stop words from line %s",
                stopWords.size(), stopWordsFile.getName()));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            if (!stopWords.contains(word.toLowerCase())) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
