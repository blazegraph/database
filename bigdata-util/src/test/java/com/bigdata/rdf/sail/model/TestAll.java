package com.bigdata.rdf.sail.model;

import static org.junit.Assert.fail;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.junit.Test;

public class TestAll extends TestCase {
	
	 public static TestSuite suite() {

	        final TestSuite suite = new TestSuite("JsonSerialization");
	        
	        suite.addTest(new TestSuite(TestJsonModelSerialization.class));
	        
	        return suite;
	 }

}
