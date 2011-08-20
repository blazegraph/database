package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Test suite for {@link FullyInlineTypedLiteralIV}.
 */
public class TestInlineLiteralIV extends TestCase2 {

	public TestInlineLiteralIV() {
	}

	public TestInlineLiteralIV(String name) {
		super(name);
	}

	public void test_InlineLiteralIV_plain() {

        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(""));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(" "));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("1"));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("12"));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("123"));

	}
	
	public void test_InlineLiteralIV_languageCode() {

        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("","en",null/*datatype*/));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ","en",null/*datatype*/));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("1","en",null/*datatype*/));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("12","fr",null/*datatype*/));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("123","de",null/*datatype*/));

	}

	public void test_InlineLiteralIV_datatypeURI() {

		final URI datatype = new URIImpl("http://www.bigdata.com");

		doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("", null, datatype));
		doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ", null, datatype));
		doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("1", null, datatype));
		doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("12", null, datatype));
		doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("123", null, datatype));

	}

	private void doTest(final FullyInlineTypedLiteralIV<BigdataLiteral> iv) {

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
