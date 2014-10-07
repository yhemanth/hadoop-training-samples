package com.dsinpractice.samples.hadoop.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogEntry implements Writable {

    private String date;
    private String time;
    private HttpRequest request;

    public String getDate() {
        return date;
    }

    public String getTime() {
        return time;
    }

    public HttpRequest getRequest() {
        return request;
    }

    public LogEntry() {

    }

    public LogEntry(String date, String time, HttpRequest request) {
        this.date = date;
        this.time = time;
        this.request = request;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(time);
        request.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date = dataInput.readUTF();
        time = dataInput.readUTF();
        request = new HttpRequest();
        request.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogEntry logEntry = (LogEntry) o;

        if (!date.equals(logEntry.date)) return false;
        if (!request.equals(logEntry.request)) return false;
        if (!time.equals(logEntry.time)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = date.hashCode();
        result = 31 * result + time.hashCode();
        result = 31 * result + request.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s", date, time, request.toString());
    }

}
