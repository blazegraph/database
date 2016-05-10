package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

public class TestInlineLocalNameIntegerURIHandler extends TestCase2 {
	
	private MyInlineLocalNameIntegerURIHandler handler = null;
	
	private int packedId = 1;
	
	public TestInlineLocalNameIntegerURIHandler(final String name) {
		super(name);
		handler = new MyInlineLocalNameIntegerURIHandler("http://www.blazegraph.com/",packedId);
	}
	

	public void test_packValues() {
		
		final long testVal = 123456789; 

		check(handler, testVal, packedId);
		
		
	}
	
	public void test_idrange() {
		
		final long value = 5555555555L;
		
		for(int i = 0; i < 32; i++) {

			final MyInlineLocalNameIntegerURIHandler h = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", i);
			
			check(h, value, i);
			
		}

		
	}
	
	public void test_maxInteger() {
		
		final long tooBig = 576460752303423000L;
		
		try {
			handler.packValue(tooBig);
		} catch (RuntimeException e) {
			//This should throw an exception
			assertTrue(true);
			return;
		}
		
		fail(tooBig +" exceeds the maximum value.");
	}
	
	public void test_maxId() {
		
		try {
			@SuppressWarnings("unused")
			MyInlineLocalNameIntegerURIHandler h = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", 33);
		} catch (RuntimeException e) {
			// This should throw an exception
			assertTrue(true);
			return;
		}

		fail(32 + " exceeds the maximum id value.");
	}
	
	/**
	 *  Test of the maximum int value edge case.
	 * 
	 */
	public void test_edgeCase1() {
		
		final long edge = 144115188075856000L - 1;
		final int id = 16;

		MyInlineLocalNameIntegerURIHandler h = new MyInlineLocalNameIntegerURIHandler(
				"www.blazegraph.com", id);

		check(h,edge,id);
	}
	
	
	public void test_edgeCase2() {
		
		final long edge = 144115188075856000L;
		final int id = 16;

	    MyInlineLocalNameIntegerURIHandler h = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", id);

		try {
			@SuppressWarnings("unused")
			final long packedVal = h.packValue(edge);
		} catch (RuntimeException e) {
			//Should fail
			assertTrue(true);
			return;
		}
		fail();
		
	}
	
	public void test_edgeCase3() {
		
		final long edge = 0;
		final int id = 16;

	    MyInlineLocalNameIntegerURIHandler h = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", id);

		try {
			@SuppressWarnings(value = { "unused" })
			final long packedVal = h.packValue(edge);
		} catch (RuntimeException e) {
			//Should not fail
			fail();
		}

		check(h,edge,id);
		
	}	

	public void test_edgeCase4() {
		
		final long edge = -1;
		final int id = 16;

	    MyInlineLocalNameIntegerURIHandler h = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", id);

		try {
			@SuppressWarnings(value = { "unused" })
			final long packedVal = h.packValue(edge);
		} catch (RuntimeException e) {
			//Should fail
			assertTrue(true);
			return;
		}

		fail();
		
	}	
	
	/**
	 * Test the two different ids with the same integer value are
	 * different packed values.
	 */
	public void test_differentIds() {
		
		final long value = 123456789;
		final int id1 = 4;
		final int id2 = 31;
		
		MyInlineLocalNameIntegerURIHandler h1 = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", id1);

		MyInlineLocalNameIntegerURIHandler h2 = new MyInlineLocalNameIntegerURIHandler(
					"www.blazegraph.com", id2);
		
		final long packed1 = h1.packValue(value);
		final long packed2 = h2.packValue(value);
	
		//Different IDs should differ
		assertTrue(packed1 != packed2);
		
		final long unpacked1 = h1.unpackValue(packed1);
		final long unpacked2 = h2.unpackValue(packed2);
		
		//These should be the same value
		assertTrue(unpacked1 == unpacked2);
		
	}
	
	private class MyInlineLocalNameIntegerURIHandler extends InlineLocalNameIntegerURIHandler {

		public MyInlineLocalNameIntegerURIHandler(String namespace, int packedId) {
			super(namespace, packedId);
		}

	}
	
	private void check(MyInlineLocalNameIntegerURIHandler h, long value, int id) {

		final long packedVal = h.packValue(value);
		
		if(log.isDebugEnabled()) {
			log.debug("packedVal: " + packedVal);
			log.debug("test:  " + value + " : " + h.unpackValue(packedVal));
			log.debug("packedId:  " + id + " : " + h.unpackId(packedVal));
		}

		assertTrue( value  == h.unpackValue(packedVal) );
		
		assertTrue ( id == h.unpackId(packedVal));

	}

}
