package com.bigdata.rdf.model;

import junit.framework.TestCase;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;

/**
 * Test suite for equals() semantics for {@link BigdataValue} implementations.
 * Each test makes sure that two bigdata values are equals() if they have the
 * same data, regardless of whether they have the same value factory. Note that
 * two {@link BigdataValue}s for the same {@link ValueFactory} which have the
 * same {@link IV} are compared on the basis of that {@link IV} (unless it is a
 * "dummy" or "mock" IV). 
 */
public class TestEquals extends TestCase {

	public TestEquals() {
		
	}

	public TestEquals(String name) {
		super(name);
	}

	public void test_equalsURI() {
		
	    final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance(getName());
	
	    final BigdataValueFactory vf2 = BigdataValueFactoryImpl.getInstance(getName()+"2");
		
	    final BigdataURI v1 = vf.createURI("http://www.bigdata.com");
	    
	    final BigdataURI v2 = vf.createURI("http://www.bigdata.com");
	    
	    final URI v3 = new URIImpl("http://www.bigdata.com");

	    final BigdataURI v4 = vf2.createURI("http://www.bigdata.com");

	    assertTrue( v1 != v2 );
	    
	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	    v2.setIV(TermId.mockIV(VTE.URI));
	    
	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	    v1.setIV(new TermId<BigdataURI>(VTE.URI, 1));

	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	}

	public void test_equalsLiteral() {

		doLiteralTest("bigdata", null/* datatype */, null/* languageCode */);

		doLiteralTest("bigdata", XMLSchema.STRING/* datatype */, null/* languageCode */);

		doLiteralTest("bigdata", null/* datatype */, "en"/* languageCode */);

	}

	private Literal createLiteral(ValueFactory f, final String label,
			final URI datatype, final String languageCode) {

		if (datatype == null && languageCode == null)
			return f.createLiteral(label);

		if (datatype == null)
			return f.createLiteral(label, languageCode);
		
		return f.createLiteral(label, datatype);

	}

	private void doLiteralTest(final String label, final URI datatype,
			final String languageCode) {

		final BigdataValueFactory vf = BigdataValueFactoryImpl
				.getInstance(getName());

		final BigdataValueFactory vf2 = BigdataValueFactoryImpl
				.getInstance(getName() + "2");

		final BigdataLiteral v1 = (BigdataLiteral) createLiteral(vf, label,
				datatype, languageCode);

		final BigdataLiteral v2 = (BigdataLiteral) createLiteral(vf, label,
				datatype, languageCode);

		final Literal v3 = createLiteral(new ValueFactoryImpl(), label,
				datatype, languageCode);

		final BigdataLiteral v4 = (BigdataLiteral) createLiteral(vf2, label,
				datatype, languageCode);

	    assertTrue( v1 != v2 );
	    
	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	    v2.setIV(TermId.mockIV(VTE.LITERAL));
	    
	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	    v1.setIV(new TermId<BigdataLiteral>(VTE.LITERAL, 1));

	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	}
	
	public void test_equalsBNode() {

	    final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance(getName());

	    final BigdataValueFactory vf2 = BigdataValueFactoryImpl.getInstance(getName()+"2");
		
	    final BigdataBNode v1 = vf.createBNode("bigdata");
	    
	    final BigdataBNode v2 = vf.createBNode("bigdata");

	    final BNode v3 = new BNodeImpl("bigdata");

	    final BigdataBNode v4 = vf2.createBNode("bigdata");

	    assertTrue( v1 != v2 );
	    
	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	    v2.setIV(TermId.mockIV(VTE.BNODE));
	    
	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	    v1.setIV(new TermId<BigdataBNode>(VTE.BNODE, 1));

	    assertTrue(v1.equals(v2));
	    assertTrue(v2.equals(v1));

	    assertTrue(v3.equals(v1));
	    assertTrue(v3.equals(v2));
	    assertTrue(v1.equals(v3));
	    assertTrue(v2.equals(v3));

	    assertTrue(v1.equals(v4));
	    assertTrue(v4.equals(v1));
	    assertTrue(v2.equals(v4));
	    assertTrue(v4.equals(v2));

	}

}
