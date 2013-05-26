package com.thoughtworks.samples.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileStatusLister extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new FileStatusLister(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        validateArgs(args);

        FileSystem fileSystem = FileSystem.get(new Configuration());
        FileStatus[] fileStatuses = fileSystem.globStatus(new Path(args[0]));
        if (fileStatuses == null) {
            System.out.println("No files found");
            return 0;
        }
        for (FileStatus status : fileStatuses) {
            System.out.println(String.format("%s %s %.2fMB %d %d", status.getPath().getName(), fileDescription(status),
                    fileSizeInMBs(status), status.getBlockSize(), status.getReplication()));
        }
        return 0;
    }

    private String fileDescription(FileStatus status) {
        String type = status.isDir() ? "Directory" : "File";
        if (status.getPath().getName().startsWith("_"))
            type = "Hidden " + type;
        return type;
    }

    private double fileSizeInMBs(FileStatus status) {
        return status.getLen() / 1024*1024.0;
    }

    private void validateArgs(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: hadoop FileStatusLister <path>");
            System.exit(-1);
        }
    }
}
