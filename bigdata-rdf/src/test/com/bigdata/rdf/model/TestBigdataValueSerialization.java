package com.bigdata.rdf.model;

import junit.framework.TestCase2;

import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.io.SerializerUtil;

/**
 * Test suite for {@link BigdataValueImpl} serialization semantics, including
 * the correct recovery of the {@link BigdataValueFactoryImpl} reference when
 * deserialized.
 */
public class TestBigdataValueSerialization extends TestCase2 {

	public TestBigdataValueSerialization() {
	}

	public TestBigdataValueSerialization(String name) {
		super(name);
	}

	public void test_roundTrip_URI() {

		doRoundTripTest(new URIImpl("http://www.bigdata.com"));
		
	}
	
	public void test_roundTrip_BNode() {

		doRoundTripTest(new BNodeImpl("12"));
		
	}

	public void test_roundTrip_Literal() {

		doRoundTripTest(new LiteralImpl("bigdata"));
		
	}

	public void test_roundTrip_xsd_string() {

		doRoundTripTest(new LiteralImpl("bigdata", XMLSchema.STRING));

	}

	public void test_roundTrip_xsd_int() {

		doRoundTripTest(new LiteralImpl("12", XMLSchema.INT));

	}

	private void doRoundTripTest(final Value v) {
		
		final String namespace = getName();
		
		final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(namespace);

		// same reference (singleton pattern).
		assertTrue(f == BigdataValueFactoryImpl.getInstance(namespace));

		// Coerce into a BigdataValue.
		final BigdataValue expected = f.asValue(v);

		assertTrue(f == expected.getValueFactory());
		
		final BigdataValue actual = (BigdataValue) SerializerUtil
				.deserialize(SerializerUtil.serialize(expected));

		// same value factory reference on the deserialized term.
		assertTrue(f == actual.getValueFactory());
		
		// Values compare as equal.
		assertTrue(expected.equals(actual));

	}
	
}
