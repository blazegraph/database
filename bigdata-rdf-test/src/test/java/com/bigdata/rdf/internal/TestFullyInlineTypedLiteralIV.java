package com.bigdata.rdf.internal;

import java.util.LinkedList;
import java.util.List;

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
public class TestFullyInlineTypedLiteralIV extends TestCase2 {

	public TestFullyInlineTypedLiteralIV() {
	}

	public TestFullyInlineTypedLiteralIV(String name) {
		super(name);
	}

	public void test_InlineLiteralIV_plain() {

        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(""));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(" "));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("1"));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("12"));
        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("123"));

	}
	
//Removed in backport.    Not used in pre-RDF 1.1 versions
//	public void test_InlineLiteralIV_languageCode() {
//
//        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("","en",null/*datatype*/));
//        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ","en",null/*datatype*/));
//        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("1","en",null/*datatype*/));
//        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("12","fr",null/*datatype*/));
//        doTest(new FullyInlineTypedLiteralIV<BigdataLiteral>("123","de",null/*datatype*/));
//
//	}

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

	public void test_encodeDecode_comparator() {
        
	    final List<IV<?,?>> ivs = new LinkedList<IV<?,?>>();
        {

            final URI datatype = new URIImpl("http://www.bigdata.com");
            
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(""));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(" "));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1"));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12"));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("123"));
            
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("","en",null/*datatype*/));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ","en",null/*datatype*/));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1","en",null/*datatype*/));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12","fr",null/*datatype*/));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("123","de",null/*datatype*/));

            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("", null, datatype));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>(" ", null, datatype));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("1", null, datatype));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("12", null, datatype));
            ivs.add(new FullyInlineTypedLiteralIV<BigdataLiteral>("123", null, datatype));

        }
        
        final IV<?, ?>[] e = ivs.toArray(new IV[0]);

        AbstractEncodeDecodeKeysTestCase.doEncodeDecodeTest(e);

        AbstractEncodeDecodeKeysTestCase.doComparatorTest(e);
    
	}
	
}
