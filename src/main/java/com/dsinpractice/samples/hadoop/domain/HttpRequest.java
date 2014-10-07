package com.dsinpractice.samples.hadoop.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HttpRequest implements WritableComparable {
    private String httpMethod;
    private String url;

    public HttpRequest(String httpMethod, String url) {
        this.httpMethod = httpMethod;
        this.url = url;
    }

    public HttpRequest() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HttpRequest that = (HttpRequest) o;

        if (!httpMethod.equals(that.httpMethod)) return false;
        if (!url.equals(that.url)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = httpMethod.hashCode();
        result = 31 * result + url.hashCode();
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(httpMethod);
        dataOutput.writeUTF(url);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        httpMethod = dataInput.readUTF();
        url = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return String.format("%s,%s", httpMethod,url);
    }

    @Override
    public int compareTo(Object o) {
        if (equals(o)) return 0;
        if ((o==null) || (o.getClass() != getClass())) return 1;
        HttpRequest otherRequest = (HttpRequest) o;
        if (url.equals(otherRequest.url)) {
            return httpMethod.compareTo(otherRequest.httpMethod);
        } else {
            return url.compareTo(otherRequest.url);
        }
    }
}
