package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import org.junit.Test;

import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;

/**
 * Test suite for {@link URIExtensionIV}.
 */
public class TestInlineURIHandlers extends TestCase2 {

	public TestInlineURIHandlers() {
	}

	public TestInlineURIHandlers(String name) {
		super(name);
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testPrefixedIntegerURIHandler() {
	
		final String nameSpace = "http://blazegraph.com/";
		final String localName = "DB1234";
		final String fixture = "DB";
		final int intValue = 1234;
		
		InlinePrefixedIntegerURIHandler handler = new InlinePrefixedIntegerURIHandler(nameSpace, fixture);
		
		XSDNumericIV iv = (XSDNumericIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (intValue == iv.getInlineValue().intValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testSuffixedIntegerURIHandler() {
	
		final String nameSpace = "http://blazegraph.com/";
		final String localName = "1234DB";
		final String fixture = "DB";
		final int intValue = 1234;
		
		InlineSuffixedIntegerURIHandler handler = new InlineSuffixedIntegerURIHandler(nameSpace, fixture);
		
		XSDNumericIV iv = (XSDNumericIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (intValue == iv.getInlineValue().intValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testFixedWidthIntegerURIHandler() {
	
		final String nameSpace = "http://blazegraph.com/";
		final String localName = "001234";
		final int intValue = 1234;
		final int width = 6;
		
		InlineFixedWidthIntegerURIHandler handler = new InlineFixedWidthIntegerURIHandler(nameSpace, width);
		
		XSDNumericIV iv = (XSDNumericIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (intValue == iv.getInlineValue().intValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testFixedWidthIntegerURIHandlerFail() {
	
		final String nameSpace = "http://blazegraph.com/";
		final String localName = "001234";
		final int intValue = 1234;
		final int width = 7;
		
		InlineFixedWidthIntegerURIHandler handler = new InlineFixedWidthIntegerURIHandler(nameSpace, width);
		
		XSDNumericIV iv = (XSDNumericIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (intValue == iv.getInlineValue().intValue());
		
		//These should be equal as the width is 7
		assertTrue (!localName.equals(handler.getLocalNameFromDelegate(iv)));

		//FIXME:  CI Debugging
		System.err.println("Format:  " + String.format("%"+width+"d",intValue));
		System.err.println("Handler:  " + handler.getLocalNameFromDelegate(iv));
		
		
		assertTrue (String.format("%"+width+"d",intValue).equals(handler.getLocalNameFromDelegate(iv)));
	}		
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testPrefixFixedWidthIntegerURIHandler() {
	
		final String nameSpace = "http://blazegraph.com/";
		final String localName = "PREFIX_001234";
		final String fixture = "PREFIX_";
		final int intValue = 1234;
		final int width = 6;
		
		InlinePrefixedFixedWidthIntegerURIHandler handler = new InlinePrefixedFixedWidthIntegerURIHandler(nameSpace, fixture, width);
		
		XSDNumericIV iv = (XSDNumericIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (intValue == iv.getInlineValue().intValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testSuffixFixedWidthIntegerURIHandler() {
	
		final String nameSpace = "http://blazegraph.com/";
		final String localName = "001234_SUFFIX";
		final String fixture = "_SUFFIX";
		final int intValue = 1234;
		final int width = 6;
		
		InlineSuffixedFixedWidthIntegerURIHandler handler = new InlineSuffixedFixedWidthIntegerURIHandler(nameSpace, fixture, width);
		
		XSDNumericIV iv = (XSDNumericIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		//FIXME:  CI Debugging
		System.err.println(iv.getDTE().name());
		
		assertTrue (iv != null);
		
		assertTrue (intValue == iv.getInlineValue().intValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}

}