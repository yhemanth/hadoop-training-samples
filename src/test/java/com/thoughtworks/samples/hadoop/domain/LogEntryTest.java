package com.thoughtworks.samples.hadoop.domain;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class LogEntryTest {

    @Test
    public void should_be_equal() {
        LogEntry entry1 = new LogEntry("29/Apr/2013", "10:20:28",
                new HttpRequest("GET", "/static/js/Source/jHue/jquery.notify.js"));
        LogEntry entry2 = new LogEntry("29/Apr/2013", "10:20:28",
                new HttpRequest("GET", "/static/js/Source/jHue/jquery.notify.js"));

        assertEquals(entry1, entry2);
    }

    @Test
    public void should_serialize_fields_correctly() throws IOException {
        LogEntry entry = new LogEntry("29/Apr/2013", "10:20:28",
                new HttpRequest("GET", "/static/js/Source/jHue/jquery.notify.js"));
        DataOutput dataOutput = mock(DataOutput.class);
        entry.write(dataOutput);

        InOrder inOrderVerifier = inOrder(dataOutput);
        inOrderVerifier.verify(dataOutput).writeUTF("29/Apr/2013");
        inOrderVerifier.verify(dataOutput).writeUTF("10:20:28");
        inOrderVerifier.verify(dataOutput).writeUTF("GET");
        inOrderVerifier.verify(dataOutput).writeUTF("/static/js/Source/jHue/jquery.notify.js");
    }

    @Test
    public void should_deserialize_fields_correctly() throws IOException {
        LogEntry expectedEntry = new LogEntry("29/Apr/2013", "10:20:28",
                new HttpRequest("GET", "/static/js/Source/jHue/jquery.notify.js"));

        DataInput dataInput = mock(DataInput.class);
        when(dataInput.readUTF()).thenReturn("29/Apr/2013")
                .thenReturn("10:20:28")
                .thenReturn("GET")
                .thenReturn("/static/js/Source/jHue/jquery.notify.js");

        LogEntry entry = new LogEntry();
        entry.readFields(dataInput);

        assertEquals(expectedEntry, entry);

    }
}
