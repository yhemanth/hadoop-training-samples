package com.thoughtworks.samples.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FileUploader extends Configured implements Tool {

    public static final int BUFFER_SIZE = 4096;
    public static final short REPLICATION = (short) 1;
    public static final long BLOCK_SIZE = 10 * 1024 * 1024L;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new FileUploader(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        validateArgs(args);

        LocalFileSystem localFs = FileSystem.getLocal(getConf());
        Path srcPath = new Path(args[0]);
        FSDataInputStream srcStream = localFs.open(srcPath);
        long fileLen = localFs.getFileStatus(srcPath).getLen();

        FileSystem fileSystem = FileSystem.get(getConf());
        FSDataOutputStream destStream = fileSystem.create(new Path(args[1]), true, BUFFER_SIZE,
                REPLICATION, BLOCK_SIZE);

        try {
            upload(srcStream, destStream, fileLen);
        } finally {
            srcStream.close();
            localFs.close();

            destStream.close();
            fileSystem.close();
        }
        return 0;
    }

    private void validateArgs(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: hadoop FileUploader <local path> <path>");
            System.exit(-1);
        }
    }

    private void upload(FSDataInputStream srcStream, FSDataOutputStream destStream, long fileSize)
            throws IOException {
        long bytesRemaining = fileSize;
        int bytesRead = 0;

        byte[] inputBuffer = new byte[BUFFER_SIZE];
        while (bytesRemaining > 0 && bytesRead != -1) {
            bytesRead = srcStream.read(inputBuffer, 0, bytesToRead(bytesRemaining, inputBuffer));
            destStream.write(inputBuffer, 0, bytesRead);
            bytesRemaining -= bytesRead;
        }
    }

    private int bytesToRead(long toRead, byte[] inputBuffer) {
        return (int) Math.min(inputBuffer.length, toRead);
    }
}
