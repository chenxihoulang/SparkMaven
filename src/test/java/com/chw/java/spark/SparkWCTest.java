package com.chw.java.spark;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class SparkWCTest {
    SparkWC sparkWC;
    @org.junit.Before
    public void setUp() throws Exception {
        sparkWC=new SparkWC();
    }

    @org.junit.After
    public void tearDown() throws Exception {
        sparkWC=null;
    }

    @org.junit.Test
    public void getName() {
        assertTrue(1==1);
    }
}