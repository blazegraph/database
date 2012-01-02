package com.bigdata.rdf.lexicon;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.bnode.NumericBNodeIV;
import com.bigdata.rdf.internal.impl.bnode.UUIDBNodeIV;
import com.bigdata.rdf.internal.impl.literal.UUIDLiteralIV;
import com.bigdata.rdf.internal.impl.literal.XSDBooleanIV;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

/**
 * Test suite for {@link IV#getValue()}, which provides a cache on the
 * {@link IV} for a materialized {@link BigdataValue}.
 * 
 * @author thompsonbry
 */
public class TestIVCache extends AbstractTripleStoreTestCase {

	public TestIVCache() {
	}

	public TestIVCache(String name) {
		super(name);
	}

	/**
	 * Unit test for {@link IV#getValue()} and friends.
	 */
    public void test_getValue() {
    	
    	final AbstractTripleStore store = getStore(getProperties());

    	try {
    		
    		final LexiconRelation lex = store.getLexiconRelation();

    		final BigdataValueFactory f = lex.getValueFactory();
    		
    		final BigdataURI uri = f.createURI("http://www.bigdata.com");
    		final BigdataBNode bnd = f.createBNode();//"12");
    		final BigdataLiteral lit = f.createLiteral("bigdata");
    		
    		final BigdataValue[] a = new BigdataValue[] {
    			uri, bnd, lit
    		};
    		
    		// insert some terms.
    		lex.addTerms(a, a.length, false/*readOnly*/);

    		doTest(lex,uri.getIV(), uri);
    		doTest(lex,bnd.getIV(), bnd);
    		doTest(lex,lit.getIV(), lit);
    		
			doTest(lex, new XSDBooleanIV<BigdataLiteral>(true));
			doTest(lex, new XSDNumericIV<BigdataLiteral>((byte)1));
			doTest(lex, new XSDNumericIV<BigdataLiteral>((short)1));
			doTest(lex, new XSDNumericIV<BigdataLiteral>(1));
			doTest(lex, new XSDNumericIV<BigdataLiteral>(1L));
			doTest(lex, new XSDNumericIV<BigdataLiteral>(1f));
			doTest(lex, new XSDNumericIV<BigdataLiteral>(1d));
			doTest(lex, new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(1L)));
			doTest(lex, new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1d)));
			
			doTest(lex, new UUIDBNodeIV<BigdataBNode>(UUID.randomUUID()));
			doTest(lex, new NumericBNodeIV<BigdataBNode>(1));
			
			doTest(lex, new UUIDLiteralIV<BigdataLiteral>(UUID.randomUUID()));
			
    	} finally {
    		
    		store.__tearDownUnitTest();
    		
    	}
    	
	}

    /**
     * Variant used *except* for {@link BlobIV}s.
     * @param lex
     * @param iv
     */
	private void doTest(final LexiconRelation lex, final IV iv) {
		
		doTest(lex,iv,null/*given*/);
		
	}

	/**
	 * Core impl.
	 * @param lex
	 * @param iv
	 * @param given
	 */
	@SuppressWarnings("unchecked")
    private void doTest(final LexiconRelation lex, final IV iv,
			final BigdataValue given) {

		// not found in the cache.
		try {
			iv.getValue();
			fail("Expecting: " + NotMaterializedException.class);
		} catch (NotMaterializedException e) {
			// ignore.
		}

		/*
		 * Set on the cache (TermId) or materialize in the cache (everthing
		 * else).
		 */
		final BigdataValue val;
		if(!iv.isInline()) {
			((AbstractIV<BigdataValue, ?>)iv).setValue(val = given);
		} else {
			val = iv.asValue(lex);
		}

		// found in the cache.
		assertTrue(val == iv.getValue());

		// round-trip (de-)serialization.
		final IV<?,?> iv2 = (IV<?,?>) SerializerUtil.deserialize(SerializerUtil
				.serialize(iv));

		// this is a distinct IV instance.
		assertTrue(iv != iv2);

        /* Note: All IVs currently send their cached value across the wire.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/337
         */
		{
//		if (iv.isInline()) {
//
//			/*
//			 * For an inline IV, we drop the cached value when it is serialized
//			 * in order to keep down the serialized object size since it is
//			 * basically free to re-materialize the Value from the IV.
//			 */
//			// not found in the cache.
//			try {
//				iv2.getValue();
//				fail("Expecting: " + NotMaterializedException.class);
//			} catch (NotMaterializedException e) {
//				// ignore.
//			}
//
//		} else {

			/*
			 * For a non-inline IV, the value is found in the cache, even though
			 * it is not the same BigdataValue.
			 */

			final BigdataValue val2 = iv2.getValue();
			// found in the cache.
			assertNotNull(val2);
			// but distinct BigdataValue
			assertTrue(val != val2);
			// but same value factory.
			assertTrue(val.getValueFactory() == val2.getValueFactory());
			// and compares as "equals".
			assertTrue(val.equals(val2));

		}

		if (iv.isInline()) {
			// Verify the cache is unchanged.
			assertTrue(val == iv.asValue(lex));
			assertTrue(val == iv.getValue());
		}

//		/*
//		 * Drop the value and verify that it is no longer found in the cache.
//		 */
//		iv.dropValue();
//		
//		// not found in the cache.
//		try {
//			iv.getValue();
//			fail("Expecting: " + NotMaterializedException.class);
//		} catch (NotMaterializedException e) {
//			// ignore.
//		}		
		
	}

}
