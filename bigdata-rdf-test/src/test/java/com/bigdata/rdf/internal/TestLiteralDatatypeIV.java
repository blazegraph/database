package com.bigdata.rdf.internal;

import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.rdf.internal.impl.AbstractIV;
import com.bigdata.rdf.internal.impl.AbstractNonInlineExtensionIVWithDelegateIV;
import com.bigdata.rdf.internal.impl.literal.FullyInlineTypedLiteralIV;
import com.bigdata.rdf.internal.impl.literal.PartlyInlineTypedLiteralIV;
import com.bigdata.rdf.lexicon.BlobsIndexHelper;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.test.MockTermIdFactory;

/**
 * Test suite for {@link PartlyInlineTypedLiteralIV}.
 */
public class TestLiteralDatatypeIV extends TestCase2 {

	public TestLiteralDatatypeIV() {
	}

	public TestLiteralDatatypeIV(String name) {
		super(name);
	}

	private MockTermIdFactory termIdFactory;
	
	@Override
	protected void setUp() throws Exception {
		
	    super.setUp();
		
	    termIdFactory = new MockTermIdFactory();
	    
	}

	@Override
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

		doTest(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
				new FullyInlineTypedLiteralIV<BigdataLiteral>(""), datatypeIV)//
				);

		doTest(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
				new FullyInlineTypedLiteralIV<BigdataLiteral>("abc"), datatypeIV)//
				);

		doTest(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
				new FullyInlineTypedLiteralIV<BigdataLiteral>(" "), datatypeIV)//
				);

		doTest(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
				new FullyInlineTypedLiteralIV<BigdataLiteral>("1"), datatypeIV)//
				);

		doTest(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
				new FullyInlineTypedLiteralIV<BigdataLiteral>("12"), datatypeIV)//
				);
		
		final IV<?,?>[] e;
		{

            final List<IV<?, ?>> ivs = new LinkedList<IV<?, ?>>();

            ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                    new FullyInlineTypedLiteralIV<BigdataLiteral>(""),
                    datatypeIV)//
            );

            ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                    new FullyInlineTypedLiteralIV<BigdataLiteral>("abc"),
                    datatypeIV)//
            );

            ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                    new FullyInlineTypedLiteralIV<BigdataLiteral>(" "),
                    datatypeIV)//
            );

            ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                    new FullyInlineTypedLiteralIV<BigdataLiteral>("1"),
                    datatypeIV)//
            );

            ivs.add(new PartlyInlineTypedLiteralIV<BigdataLiteral>(
                    new FullyInlineTypedLiteralIV<BigdataLiteral>("12"),
                    datatypeIV)//
            );

	        e = ivs.toArray(new IV[0]);
	        
		}
		
        TestEncodeDecodeKeys.doEncodeDecodeTest(e);
        TestEncodeDecodeKeys.doComparatorTest(e);
		
	}

	private void doTest(final PartlyInlineTypedLiteralIV<BigdataLiteral> iv) {

		assertEquals(VTE.LITERAL, iv.getVTE());
		
		assertFalse(iv.isInline());
		
		assertTrue(iv.isExtension());

		assertEquals(DTE.XSDString, iv.getDTE());
		
		final BlobsIndexHelper h = new BlobsIndexHelper();
		
		final IKeyBuilder keyBuilder = h.newKeyBuilder();
		
		final byte[] key = IVUtility.encode(keyBuilder, iv).getKey();
		
		final PartlyInlineTypedLiteralIV<BigdataLiteral> actual = 
			(PartlyInlineTypedLiteralIV<BigdataLiteral>) IVUtility.decode(key);

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
