package com.thoughtworks.samples.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogEntry implements Writable {

    private String date;
    private String time;
    private String httpMethod;
    private String query;
    private String clientIp;
    private int httpStatus;
    private int timeTaken;

    public LogEntry(String date, String time, String httpMethod, String query, String clientIp,
                    int httpStatus, int timeTaken) {

        this.date = date;
        this.time = time;
        this.httpMethod = httpMethod;
        this.query = query;
        this.clientIp = clientIp;
        this.httpStatus = httpStatus;
        this.timeTaken = timeTaken;
    }

    public LogEntry() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        new Text(date).write(dataOutput);
        new Text(time).write(dataOutput);
        new Text(httpMethod.toLowerCase()).write(dataOutput);
        new Text(query.toLowerCase()).write(dataOutput);
        new Text(clientIp).write(dataOutput);
        new VIntWritable(httpStatus).write(dataOutput);
        new VIntWritable(timeTaken).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date = readString(dataInput);
        time = readString(dataInput);
        httpMethod = readString(dataInput);
        query = readString(dataInput);
        clientIp = readString(dataInput);
        httpStatus = readVInt(dataInput);
        timeTaken = readVInt(dataInput);
    }

    private int readVInt(DataInput dataInput) throws IOException {
        VIntWritable vIntWritable = new VIntWritable();
        vIntWritable.readFields(dataInput);
        return vIntWritable.get();
    }

    private String readString(DataInput dataInput) throws IOException {
        Text text = new Text();
        text.readFields(dataInput);
        return text.toString();
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%d,%d", date, time, httpMethod, query, clientIp, httpStatus, timeTaken);
    }

    public static void main(String[] args) throws IOException {
        LogEntry logEntry = new LogEntry("2012-01-01", "11:00", "POST", "/JourneyPlanningService/Booking.svc",
                "10.1.1.1", 200, 120);
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(args[0]);
        FSDataOutputStream outStream = fileSystem.create(path);
        logEntry.write(outStream);
        outStream.close();
        FSDataInputStream inputStream = fileSystem.open(path);
        LogEntry logEntry1 = new LogEntry();
        logEntry1.readFields(inputStream);
        System.out.println(logEntry);
        System.out.println(logEntry1);
    }
}
