/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jun 7, 2011
 */
package com.bigdata.rdf.internal;

import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.TermsIndexHelper;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataURI;

/**
 * Unit tests for {@link TermId}.
 * 
 * @author thompsonbry
 * 
 *         TODO Test toString() and fromString() for NullIV's. What should those
 *         methods do?
 */
public class TestTermId extends TestCase2 {

	public TestTermId() {
	}

	public TestTermId(String name) {
		super(name);
	}

	private TermsIndexHelper helper;
	
	protected void setUp() throws Exception {
		super.setUp();
		helper = new TermsIndexHelper();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		helper = null;
	}

	/**
	 * Unit test for {@link TermId#NullIV}.
	 */
    public void test_TermId_isNullIV() {
    	
    	final TermId<?> iv = TermId.NullIV;
    	
    	assertTrue(iv.isNullIV());
    	
		assertEquals(VTE.URI, iv.getVTE());

		assertEquals(0, iv.hashCode());

		assertEquals(iv, TermId.fromString(iv.toString()));
		
    }

    public void test_TermId_URI() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.URI;
    	
    	final int hashCode = 12;
    	
    	final int counter = 1;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);
		
        assertEquals(TermsIndexHelper.TERMS_INDEX_KEY_SIZE, key.length);

        final TermId<?> iv = new TermId<BigdataURI>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());
		
        assertEquals(iv, TermId.fromString(iv.toString()));

        final TermId<?> iv2 = (TermId<?>) IVUtility.decode(key);

        assertEquals(vte, iv2.getVTE());

        assertEquals(hashCode, iv2.hashCode());
        
        assertEquals(counter, iv2.counter());

        assertEquals(iv, TermId.fromString(iv2.toString()));

        assertEquals(iv, iv2);

    }

    public void test_TermId_Literal() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.LITERAL;
    	
    	final int hashCode = 12;
    	
    	final int counter = 1;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);

		final TermId<BigdataBNode> iv = new TermId<BigdataBNode>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());

		assertEquals(iv,TermId.fromString(iv.toString()));
		
    }

    public void test_TermId_BNode() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.BNODE;
    	
    	final int hashCode = 12;
    	
    	final int counter = 1;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);

		final TermId<BigdataBNode> iv = new TermId<BigdataBNode>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());
		
		assertEquals(iv,TermId.fromString(iv.toString()));

    }

    public void test_TermId_URI_Counter_ZERO() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.URI;
    	
    	final int hashCode = 12;
    	
    	final int counter = 0;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);

		final TermId<BigdataBNode> iv = new TermId<BigdataBNode>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());

		assertEquals(iv,TermId.fromString(iv.toString()));
		
    }

    public void test_TermId_URI_Counter_ONE() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.URI;
    	
    	final int hashCode = 12;
    	
    	final int counter = 1;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);

		final TermId<BigdataBNode> iv = new TermId<BigdataBNode>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());

		assertEquals(iv,TermId.fromString(iv.toString()));
		
    }

    public void test_TermId_URI_Counter_MIN_VALUE() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.URI;
    	
    	final int hashCode = 12;
    	
    	final int counter = Byte.MIN_VALUE;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);

		final TermId<BigdataBNode> iv = new TermId<BigdataBNode>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());

		assertEquals(iv,TermId.fromString(iv.toString()));
		
    }

    public void test_TermId_URI_Counter_MAX_VALUE() {
    	
    	final IKeyBuilder keyBuilder = helper.newKeyBuilder();
    	
    	final VTE vte = VTE.URI;
    	
    	final int hashCode = 12;
    	
    	final int counter = Byte.MAX_VALUE;
    	
		final byte[] key = helper.makeKey(keyBuilder, vte, hashCode,
				counter);

		final TermId<BigdataBNode> iv = new TermId<BigdataBNode>(key);

		assertEquals(vte, iv.getVTE());

		assertEquals(hashCode, iv.hashCode());
		
		assertEquals(counter, iv.counter());

		assertEquals(iv,TermId.fromString(iv.toString()));
		
    }

    /**
     * Unit test for {@link TermId#mockIV(VTE)}.
     */
    public void test_TermId_mockIV() {

    	for(VTE vte : VTE.values()) {
    		
    		final TermId<?> v = TermId.mockIV(vte);
    		
			assertTrue(v.isTermId());

			assertFalse(v.isInline());

			assertTrue(v.isNullIV());

			assertEquals(vte, v.getVTE());

			assertEquals(0/* hashCode */, v.hashCode());

			assertEquals(0/* counter */, v.counter());

			// assertEquals(termId, v.getTermId());

			try {
				v.getInlineValue();
				fail("Expecting " + UnsupportedOperationException.class);
			} catch (UnsupportedOperationException ex) {
				// ignored.
			}

			switch (vte) {
			case URI:
				assertTrue(v.isURI());
				assertFalse(v.isBNode());
				assertFalse(v.isLiteral());
				assertFalse(v.isStatement());
				break;
			case BNODE:
				assertFalse(v.isURI());
				assertTrue(v.isBNode());
				assertFalse(v.isLiteral());
				assertFalse(v.isStatement());
				break;
			case LITERAL:
				assertFalse(v.isURI());
				assertFalse(v.isBNode());
				assertTrue(v.isLiteral());
				assertFalse(v.isStatement());
				break;
			case STATEMENT:
				assertFalse(v.isURI());
				assertFalse(v.isBNode());
				assertFalse(v.isLiteral());
				assertTrue(v.isStatement());
				break;
			default:
				fail("vte=" + vte);
			}

			// Verify that toString and fromString work,
			{
			
				final String s = v.toString();

				final TermId<?> tmp = TermId.fromString(s);

				// re-encode to the same string.
				assertEquals(s, tmp.toString());
				
				// TermIds compare as equals()
				assertEquals(v, tmp);
				
			}
			
    	}
    	
    }
    
    /**
     * Unit tests for {@link TermId}
     */
    public void test_TermId() {

		final Random r = new Random();

		final IKeyBuilder keyBuilder = helper.newKeyBuilder();

		for (VTE vte : VTE.values()) {

			final int hashCode = r.nextInt();

			// TODO Change range if the counter can overflow.
			final int counter = 127 - r.nextInt(255);

			final byte[] key = helper.makeKey(keyBuilder.reset(), vte,
					hashCode, counter);

			final TermId<?> v = new TermId(key);

			assertTrue(v.isTermId());

			assertFalse(v.isInline());

			if (!vte.equals(v.getVTE())) {
				fail("Expected=" + vte + ", actual=" + v.getVTE() + " : iv="
						+ v+", key="+BytesUtil.toString(key));
			}

			assertEquals(hashCode, v.hashCode());

			assertEquals(counter, v.counter());
			
			// assertEquals(termId, v.getTermId());

			try {
				v.getInlineValue();
				fail("Expecting " + UnsupportedOperationException.class);
			} catch (UnsupportedOperationException ex) {
				// ignored.
			}

			switch (vte) {
			case URI:
				assertTrue(v.isURI());
				assertFalse(v.isBNode());
				assertFalse(v.isLiteral());
				assertFalse(v.isStatement());
				break;
			case BNODE:
				assertFalse(v.isURI());
				assertTrue(v.isBNode());
				assertFalse(v.isLiteral());
				assertFalse(v.isStatement());
				break;
			case LITERAL:
				assertFalse(v.isURI());
				assertFalse(v.isBNode());
				assertTrue(v.isLiteral());
				assertFalse(v.isStatement());
				break;
			case STATEMENT:
				assertFalse(v.isURI());
				assertFalse(v.isBNode());
				assertFalse(v.isLiteral());
				assertTrue(v.isStatement());
				break;
			default:
				fail("vte=" + vte);
			}

			// Verify that toString and fromString work,
			{
			
				final String s = v.toString();

				final TermId<?> tmp = TermId.fromString(s);

				assertEquals(v, tmp);
				
			}
			
		}

	}

    public void test_NullIV_decode_as_null_reference() {
        
        final TermsIndexHelper h = new TermsIndexHelper();
        
        final IV iv = TermId.NullIV;
        
        final IKeyBuilder keyBuilder = h.newKeyBuilder();

        final byte[] key = IVUtility.encode(keyBuilder, iv).getKey();
        
        assertNull(IVUtility.decode(key));
        
    }
    
}
