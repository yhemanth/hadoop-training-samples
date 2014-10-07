package com.dsinpractice.samples.hadoop.domain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LogEntries implements Writable {

    List<LogEntry> logentries = new ArrayList<LogEntry>();

    public LogEntries(List<LogEntry> logEntries) {
        this.logentries = logEntries;
    }

    public LogEntries() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(logentries.size());
        for (LogEntry entry : logentries) {
            entry.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        logentries.clear();
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            LogEntry logEntry = new LogEntry();
            logEntry.readFields(dataInput);
            logentries.add(logEntry);
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (LogEntry entry : logentries) {
            sb.append(entry.toString()).append("\n");
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        List<LogEntry> entries = new ArrayList<LogEntry>();
        entries.add(new LogEntry("29/Apr/2013", "10:20:28",
                new HttpRequest("GET", "/static/js/Source/jHue/jquery.notify.js")));
        entries.add(new LogEntry("29/Apr/2013", "10:35:31",
                new HttpRequest("GET", "/debug/check_config_ajax")));
        LogEntries logEntries = new LogEntries(entries);
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(args[0]);
        FSDataOutputStream outStream = fileSystem.create(path);
        logEntries.write(outStream);
        outStream.close();
        FSDataInputStream inputStream = fileSystem.open(path);
        LogEntries logEntries1 = new LogEntries();
        logEntries1.readFields(inputStream);
        System.out.println(logEntries1);
    }
}
