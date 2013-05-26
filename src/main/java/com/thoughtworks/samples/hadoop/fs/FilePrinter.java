package com.thoughtworks.samples.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FilePrinter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new FilePrinter(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        validateArgs(args);

        FileSystem fileSystem = FileSystem.get(getConf());
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(args[0]));
        long blockSize = fileStatus.getBlockSize();
        long fileSize = fileStatus.getLen();

        double numBlocks = Math.ceil((float)fileSize / blockSize);
        System.out.println("Number of blocks: " + numBlocks);

        FSDataInputStream inputStream = fileSystem.open(new Path(args[0]));
        try {
            for (int i=0; i<numBlocks; i++) {
                printBlock(i, blockSize, inputStream);
                System.out.print(":");
                System.in.read();
            }
        } finally {
            inputStream.close();
            fileSystem.close();
        }

        return 0;
    }

    private void validateArgs(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: hadoop FilePrinter <filename>");
            System.exit(-1);
        }
    }

    private static void printBlock(int blockNumber, long blockSize, FSDataInputStream inputStream) throws IOException {

        System.out.println("Block #" + blockNumber);

        byte[] buffer = new byte[4096];
        long bytesRemaining = blockSize;
        int bytesRead = 0;

        while (bytesRemaining > 0 && bytesRead != -1) {
            bytesRead = inputStream.read(buffer, 0, (int) Math.min(buffer.length, bytesRemaining));
            if (bytesRead > 0)
                System.out.println(new String(buffer, 0, bytesRead));
            bytesRemaining -= bytesRead;
        }
    }

}
