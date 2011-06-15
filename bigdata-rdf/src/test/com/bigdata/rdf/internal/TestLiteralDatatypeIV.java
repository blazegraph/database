package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.lexicon.TermsIndexHelper;
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
     * Factory for {@link TermId}s.
     */
    private TermId newTermId(final VTE vte) {
        return termIdFactory.newTermId(vte);
    }

	public void test_LiteralDatatypeIV() {

		final TermId<?> datatypeIV = newTermId(VTE.URI);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>(""), datatypeIV),//
				false//isInline
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>("abc"), datatypeIV),//
				false//isInline
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>(" "), datatypeIV),//
				false//isInline
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>("1"), datatypeIV),//
				false//isInline
				);

		doTest(new LiteralDatatypeIV<BigdataLiteral>(
				new InlineLiteralIV<BigdataLiteral>("12"), datatypeIV),//
				false//isInline
				);

	}

	private void doTest(final LiteralDatatypeIV<BigdataLiteral> iv,
			final boolean isInline) {

		assertEquals(VTE.LITERAL, iv.getVTE());
		
		assertEquals(isInline, iv.isInline());
		
		assertTrue(iv.isExtension());

		assertEquals(DTE.XSDString, iv.getDTE());
		
		final TermsIndexHelper h = new TermsIndexHelper();
		
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

			final AbstractIV<?, ?> actualDelegateIV = ((AbstractExtensionIV<?, ?>) actual)
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
