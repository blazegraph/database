package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Test suite for {@link InlineLiteralIV}.
 */
public class TestInlineLiteralIV extends TestCase2 {

	public TestInlineLiteralIV() {
	}

	public TestInlineLiteralIV(String name) {
		super(name);
	}

	public void test_InlineLiteralIV_plain() {

        doTest(new InlineLiteralIV<BigdataLiteral>(""));
        doTest(new InlineLiteralIV<BigdataLiteral>(" "));
        doTest(new InlineLiteralIV<BigdataLiteral>("1"));
        doTest(new InlineLiteralIV<BigdataLiteral>("12"));
        doTest(new InlineLiteralIV<BigdataLiteral>("123"));

	}
	
	public void test_InlineLiteralIV_languageCode() {

        doTest(new InlineLiteralIV<BigdataLiteral>("","en",null/*datatype*/));
        doTest(new InlineLiteralIV<BigdataLiteral>(" ","en",null/*datatype*/));
        doTest(new InlineLiteralIV<BigdataLiteral>("1","en",null/*datatype*/));
        doTest(new InlineLiteralIV<BigdataLiteral>("12","fr",null/*datatype*/));
        doTest(new InlineLiteralIV<BigdataLiteral>("123","de",null/*datatype*/));

	}

	public void test_InlineLiteralIV_datatypeURI() {

		final URI datatype = new URIImpl("http://www.bigdata.com");

		doTest(new InlineLiteralIV<BigdataLiteral>("", null, datatype));
		doTest(new InlineLiteralIV<BigdataLiteral>(" ", null, datatype));
		doTest(new InlineLiteralIV<BigdataLiteral>("1", null, datatype));
		doTest(new InlineLiteralIV<BigdataLiteral>("12", null, datatype));
		doTest(new InlineLiteralIV<BigdataLiteral>("123", null, datatype));

	}

	private void doTest(final InlineLiteralIV<BigdataLiteral> iv) {

		assertEquals(VTE.LITERAL, iv.getVTE());
		
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
