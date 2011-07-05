package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;

/**
 * Test suite for {@link InlineURIIV}.
 */
public class TestInlineURIIV extends TestCase2 {

	public TestInlineURIIV() {
	}

	public TestInlineURIIV(String name) {
		super(name);
	}

	public void test_InlineURIIV() {

        doTest(new InlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com")));
        doTest(new InlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com/")));
        doTest(new InlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com/foo")));
        doTest(new InlineURIIV<BigdataURI>(new URIImpl("http://www.bigdata.com:80/foo")));

	}

	private void doTest(final InlineURIIV<BigdataURI> iv) {

		assertEquals(VTE.URI, iv.getVTE());
		
		assertTrue(iv.isInline());
		
		assertFalse(iv.isExtension());

		assertEquals(DTE.XSDString, iv.getDTE());
		
		final BlobsIndexHelper h = new BlobsIndexHelper();
		
		final IKeyBuilder keyBuilder = h.newKeyBuilder();
		
		final byte[] key = IVUtility.encode(keyBuilder, iv).getKey();
		
		final IV<?,?> actual = IVUtility.decode(key);
		
		assertEquals(iv, actual);
		
		assertEquals(key.length, iv.byteLength());

		assertEquals(key.length, actual.byteLength());
		
	}
	
}
