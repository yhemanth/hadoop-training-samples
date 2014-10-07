package com.dsinpractice.samples.hadoop.domain;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class HttpRequestTest {

    @Test
    public void should_be_equal_to_same_request() {
        HttpRequest httpRequest1 = new HttpRequest("GET", "url");
        HttpRequest httpRequest2 = new HttpRequest("GET", "url");
        assertEquals(0, httpRequest1.compareTo(httpRequest2));
    }

    @Test
    public void post_should_be_more_than_get_request() {
        HttpRequest httpRequest1 = new HttpRequest("POST", "url");
        HttpRequest httpRequest2 = new HttpRequest("GET", "url");
        assertTrue(httpRequest1.compareTo(httpRequest2)>0);
    }

    @Test
    public void get_should_be_less_than_post_request() {
        HttpRequest httpRequest1 = new HttpRequest("POST", "url");
        HttpRequest httpRequest2 = new HttpRequest("GET", "url");
        assertTrue(httpRequest2.compareTo(httpRequest1)<0);
    }

    @Test
    public void url_should_be_lexically_compared() {
        HttpRequest httpRequest1 = new HttpRequest("POST", "a");
        HttpRequest httpRequest2 = new HttpRequest("POST", "b");
        assertTrue(httpRequest1.compareTo(httpRequest2)<0);
    }

    @Test
    public void object_is_greater_than_null() {
        assertTrue(new HttpRequest("POST", "a").compareTo(null)>0);
    }
}
