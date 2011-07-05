package com.bigdata.rdf.model;

import java.util.UUID;

import junit.framework.TestCase2;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.io.SerializerUtil;

/**
 * Test suite for {@link BigdataValueImpl} serialization semantics, including
 * the correct recovery of the {@link BigdataValueFactoryImpl} reference when
 * deserialized.
 * 
 * @see BigdataValueSerializer
 */
public class TestBigdataValueSerialization extends TestCase2 {

	public TestBigdataValueSerialization() {
	}

	public TestBigdataValueSerialization(String name) {
		super(name);
	}

    /**
     * Fixture under test.
     */
    private BigdataValueSerializer<Value> fixture = null;
    
    protected void setUp() throws Exception {
        
        super.setUp();
        
        fixture = new BigdataValueSerializer<Value>(
                ValueFactoryImpl.getInstance());
        
    }
    
    protected void tearDown() throws Exception {

        fixture = null;
        
        super.tearDown();
        
    }
    
    /**
     * Performs round trip (de-)serialization using
     * {@link BigdataValueSerializer#serialize()} and
     * {@link BigdataValueSerializer#deserialize(byte[])}.
     * 
     * @param o
     *            The {@link Value}
     * 
     * @return The de-serialized {@link Value}.
     */
    private Value roundTrip_tuned(final Value o) {
        
        return fixture.deserialize(fixture.serialize(o));
        
    }
    
    /**
     * Test round trip of some URIs.
     */
    public void test_URIs() {

        final URI a = new URIImpl("http://www.bigdata.com");
        
        assertEquals(a, roundTrip_tuned(a));
        
    }
    
    /**
     * Test round trip of some plain literals.
     */
    public void test_plainLiterals() {

        final Literal a = new LiteralImpl("bigdata");
        
        assertEquals(a, roundTrip_tuned(a));
        
    }
    
    /**
     * Test round trip of some language code literals.
     */
    public void test_langCodeLiterals() {

        final Literal a = new LiteralImpl("bigdata","en");
        
        assertEquals(a, roundTrip_tuned(a));
        
    }

    /**
     * Test round trip of some datatype literals.
     */
    public void test_dataTypeLiterals() {

        final Literal a = new LiteralImpl("bigdata", XMLSchema.INT);
        
        assertEquals(a, roundTrip_tuned(a));
        
    }

    /*
     * Note: BNode serialization has been disabled since we never write
     * them on the database.
     */
    /**
     * Test round trip of some bnodes.
     */
    public void test_bnodes() {

        final BNode a = new BNodeImpl(UUID.randomUUID().toString());
        
        assertEquals(a, roundTrip_tuned(a));
        
    }

	public void test_roundTrip_URI() {

		doRoundTripTest(new URIImpl("http://www.bigdata.com"));
		
	}

	public void test_roundTrip_BNode() {

        doRoundTripTest(new BNodeImpl("12"));

        doRoundTripTest(new BNodeImpl(UUID.randomUUID().toString()));
		
	}

	public void test_roundTrip_plainLiteral() {

		doRoundTripTest(new LiteralImpl("bigdata"));
		
	}

    public void test_roundTrip_langCodeLiterals() {

        doRoundTripTest(new LiteralImpl("bigdata", "en"));

    }
	
	public void test_roundTrip_xsd_string() {

		doRoundTripTest(new LiteralImpl("bigdata", XMLSchema.STRING));

	}

	public void test_roundTrip_xsd_int() {

		doRoundTripTest(new LiteralImpl("12", XMLSchema.INT));

	}

    public void test_roundTrip_veryLargeLiteral() {

        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }

        final String s = sb.toString();

        if (log.isInfoEnabled())
            log.info("length(s)=" + s.length());
        
	    doRoundTripTest(new LiteralImpl(s));
	    
	}
	
	private void doRoundTripTest(final Value v) {
		
		final String namespace = getName();
		
		final BigdataValueFactory f = BigdataValueFactoryImpl.getInstance(namespace);

		// same reference (singleton pattern).
		assertTrue(f == BigdataValueFactoryImpl.getInstance(namespace));

		// Coerce into a BigdataValue.
		final BigdataValue expected = f.asValue(v);

		assertTrue(f == expected.getValueFactory());

		// test default java serialization.
        final BigdataValue actual1 = doDefaultJavaSerializationTest(expected);

        // same value factory reference on the deserialized term.
        assertTrue(f == actual1.getValueFactory());

        // test BigdataValueSerializer
        final BigdataValue actual2 = doBigdataValueSerializationTest(expected);

        // same value factory reference on the deserialized term.
        assertTrue(f == actual2.getValueFactory());

    }

    /**
     * Test of default Java Serialization (on an ObjectOutputStream).
     */
    private BigdataValue doDefaultJavaSerializationTest(
            final BigdataValue expected) {

        // serialize
        final byte[] data = SerializerUtil.serialize(expected);

        // deserialize
        final BigdataValue actual = (BigdataValue) SerializerUtil
                .deserialize(data);

        // Values compare as equal.
        assertTrue(expected.equals(actual));

        return actual;

    }

    /**
     * Test of {@link BigdataValueSerializer}.
     */
    private BigdataValue doBigdataValueSerializationTest(
            final BigdataValue expected) {

        final BigdataValueSerializer<BigdataValue> ser = expected
                .getValueFactory().getValueSerializer();
        
        // serialize
        final byte[] data = ser.serialize(expected);

        // deserialize
        final BigdataValue actual = ser.deserialize(data);

        // Values compare as equal.
        assertTrue(expected.equals(actual));

        return actual;

    }

}
