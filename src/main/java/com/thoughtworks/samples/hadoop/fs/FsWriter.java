package com.thoughtworks.samples.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FsWriter {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: hadoop FsWriter <path>");
            System.exit(-1);
        }
        Path path = new Path(args[0]);
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        fsDataOutputStream.write("This is a test file\n".getBytes());
        fsDataOutputStream.close();
        fileSystem.close();
    }
}
