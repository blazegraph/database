package com.blazegraph.vocab.freebase;

import java.math.BigInteger;

import junit.framework.TestCase2;

import org.junit.Test;

import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.blazegraph.vocab.freebase.InlineFreebaseGUIDMIDURIHandler;

/**
 * Test suite for {@link URIExtensionIV}.
 */
public class TestInlineFreebaseGUIDMIDURIHandler extends TestCase2 {

	public TestInlineFreebaseGUIDMIDURIHandler() {
	}

	public TestInlineFreebaseGUIDMIDURIHandler(String name) {
		super(name);
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testMIDURIHandler() {
	
		final String nameSpace = "http://rdf.freebase.com/ns/";
		//final String uri = "http://rdf.freebase.com/ns/m.0_zdd15";
		final String localName = "m.0_zdd15";
		final BigInteger longValue = new BigInteger("1" + Long.toString(InlineFreebaseGUIDMIDURIHandler.midToLong(localName)));
		
		InlineFreebaseGUIDMIDURIHandler handler = new InlineFreebaseGUIDMIDURIHandler(nameSpace);
		
		XSDIntegerIV iv = (XSDIntegerIV) handler.createInlineIV(localName);

		assertTrue (iv != null);

		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (longValue.longValue() == iv.getInlineValue().longValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testGUIDURIHandler() {
	
		final String nameSpace = "http://rdf.freebase.com/ns/";
		//ttp://rdf.freebase.com/ns/g.121k2cfp
		final String localName = "g.121k2cfp";
		final BigInteger longValue = new BigInteger("2" + Long.toString(InlineFreebaseGUIDMIDURIHandler.midToLong(localName)));

		
		InlineFreebaseGUIDMIDURIHandler handler = new InlineFreebaseGUIDMIDURIHandler(nameSpace);
		
		XSDIntegerIV iv = (XSDIntegerIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (longValue.longValue() == iv.getInlineValue().longValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
	}	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testNSGUIDURIHandler() {
	
		final String nameSpace = "http://rdf.freebase.com/ns/g.1";
		//http://rdf.freebase.com/ns/g.121k2cfp
		final String localName = "21k2cfp";
		final BigInteger longValue = new BigInteger("3" + Long.toString(InlineFreebaseGUIDMIDURIHandler.midToLong(localName)));

		
		InlineFreebaseGUIDMIDURIHandler handler = new InlineFreebaseGUIDMIDURIHandler(nameSpace);
		
		XSDIntegerIV iv = (XSDIntegerIV) handler.createInlineIV(localName);
		
		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (longValue.longValue() == iv.getInlineValue().longValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
	}	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testNSMIDURIHandler() {
	
		final String nameSpace = "http://rdf.freebase.com/ns/m.0";
		//final String uri = "http://rdf.freebase.com/ns/m.0_zdd15";
		final String localName = "_zdd15";
		final BigInteger longValue = new BigInteger("3" + Long.toString(InlineFreebaseGUIDMIDURIHandler.midToLong(localName)));
		
		InlineFreebaseGUIDMIDURIHandler handler = new InlineFreebaseGUIDMIDURIHandler(nameSpace);
		
		XSDIntegerIV iv = (XSDIntegerIV) handler.createInlineIV(localName);

		assertTrue (iv != null);

		if(log.isDebugEnabled()) {
			log.debug(iv.getDTE().name());
		}
		
		assertTrue (longValue.longValue() == iv.getInlineValue().longValue());
		
		assertTrue (localName.equals(handler.getLocalNameFromDelegate(iv)));
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test	
	public void testBadNSMIDURIHandler() {
	
		final String nameSpace = "http://rdf.freebase.com/ns/m.0";
		//This value should not inline
		//http://rdf.freebase.com/ns/m.01268se
		final String localName = "1268se";

		
		InlineFreebaseGUIDMIDURIHandler handler = new InlineFreebaseGUIDMIDURIHandler(nameSpace);
		
		XSDIntegerIV iv = (XSDIntegerIV) handler.createInlineIV(localName);

		assertTrue (iv == null);
		
	}
	
}