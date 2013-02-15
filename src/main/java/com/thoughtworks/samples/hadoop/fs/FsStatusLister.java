package com.thoughtworks.samples.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FsStatusLister {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: hadoop FsStatusLister <path>");
        }
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(args[0]));
        if (fileStatuses == null) {
            System.out.println("No files found");
            return;
        }
        for (FileStatus status : fileStatuses) {
            String type = status.isDir() ? "Directory" : "File";
            System.out.println(String.format("%s %s %.2fKb %d", status.getPath().getName(), type, status.getLen()/1024.0,
                    status.getReplication()));
        }
    }
}
