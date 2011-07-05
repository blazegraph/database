package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Test suite for {@link LiteralDatatypeIV}.
 */
public class TestLiteralDatatypeIV extends TestCase2 {

	public TestLiteralDatatypeIV() {
	}

	public TestLiteralDatatypeIV(String name) {
		super(name);
	}

	private MockTermIdFactory termIdFactory;
	
	protected void setUp() throws Exception {
		
	    super.setUp();
		
	    termIdFactory = new MockTermIdFactory();
	    
	}

	protected void tearDown() throws Exception {
		
	    super.tearDown();

	    termIdFactory = null;
	    
	}

    /**
     * Factory for mock {@link IV}s.
     */
    private IV<?,?> newTermId(final VTE vte) {
       
        return termIdFactory.newTermId(vte);
        
    }

	public void test_LiteralDatatypeIV() {

		final IV<?,?> datatypeIV = newTermId(VTE.URI);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>(""), datatypeIV)//
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>("abc"), datatypeIV)//
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>(" "), datatypeIV)//
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>("1"), datatypeIV)//
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>("12"), datatypeIV)//
				);

	}

	private void doTest(final LiteralDatatypeIV<BigdataLiteral> iv) {

		assertEquals(VTE.LITERAL, iv.getVTE());
		
		assertFalse(iv.isInline());
		
		assertTrue(iv.isExtension());

		assertEquals(DTE.XSDString, iv.getDTE());
		
		final BlobsIndexHelper h = new BlobsIndexHelper();
		
		final IKeyBuilder keyBuilder = h.newKeyBuilder();
		
		final byte[] key = IVUtility.encode(keyBuilder, iv).getKey();
		
		final AbstractIV<?, ?> actual = (AbstractIV<?, ?>) IVUtility
				.decode(key);

		// Check the extension IVs for consistency.
		{
		
			final AbstractIV<?, ?> expectedExtensionIV = (AbstractIV<?, ?>) iv
					.getExtensionIV();

			final AbstractIV<?, ?> actualExtensionIV = (AbstractIV<?, ?>) actual
					.getExtensionIV();

			assertEquals(expectedExtensionIV, actualExtensionIV);

			assertEquals(expectedExtensionIV.byteLength(), actualExtensionIV
					.byteLength());

		}
		
		// Check the delegate IVs for consistency.
		{

			final AbstractIV<?, ?> expectedDelegateIV = (AbstractIV<?, ?>) iv
					.getDelegate();

			final AbstractIV<?, ?> actualDelegateIV = ((AbstractNonInlineExtensionIVWithDelegateIV<?, ?>) actual)
					.getDelegate();

			assertEquals(expectedDelegateIV, actualDelegateIV);

			assertEquals(expectedDelegateIV.byteLength(), actualDelegateIV
					.byteLength());
			
		}
		
		// Check the total IVs for consistency.
		{
		
			assertEquals(iv, actual);

			assertEquals(key.length, iv.byteLength());

			assertEquals(key.length, actual.byteLength());

		}

	}

}
