package com.bigdata.rdf.internal;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.vocab.TestMultiVocabulary;
import com.bigdata.rdf.vocab.TestNamespaceMultiURIHandler;

/**
 *
 * Test case for multiple InlineURIHandlers at a single namespace.
 * 
 * <pre>
 *
 * 		http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E
 * 		http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_TaxCost
 * 		http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_UnrealizedGain
 * 		http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_WashSale
 * </pre>
 * 
 * {@see https://jira.blazegraph.com/browse/BLZG-1938}
 * 
 * @author beebs
 *
 */
public class TestMultiInlineURIHandlersSingleNamespace extends
		AbstractTripleStoreTestCase {

	

	/**
	 * Please set your database properties here, except for your journal file,
	 * please DO NOT SPECIFY A JOURNAL FILE.
	 */
	@Override
	public Properties getProperties() {

		final Properties props = new Properties(super.getProperties());

		/*
		 * Turn off inference.
		 */
		props.setProperty(BigdataSail.Options.AXIOMS_CLASS,
				NoAxioms.class.getName());
		props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.setProperty(BigdataSail.Options.JUSTIFY, "false");

		// Test with TestVocabulary Vocabulary
		props.setProperty(Options.VOCABULARY_CLASS,
				TestMultiVocabulary.class.getName());

		// Test with TestVocabulary InlineURIHandler
		props.setProperty(Options.INLINE_URI_FACTORY_CLASS,
				TestNamespaceMultiURIHandler.class.getName());

		// test w/o axioms - they imply a predefined vocab.
		props.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());

		// test w/o the full text index.
		props.setProperty(Options.TEXT_INDEX, "false");

		return props;

	}
	
	public void test_TwoNamespaceCreation() {
		
		final InlineNamespaceMultiURIHandler mHandler = new InlineNamespaceMultiURIHandler("http://blazegraph.com/data/");
		boolean noException = true;
		
		try {
			InlineSignedIntegerURIHandler i = new InlineSignedIntegerURIHandler("http://www.bigdata.com/");
			mHandler.addHandler(i);
		} catch (RuntimeException e) {
			noException = false;
		}
		
		if(noException)
			fail();

	}

	public void test_TestVocabularyInlineValues() {

		final Properties properties = getProperties();

		AbstractTripleStore store = getStore(properties);

		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, 4 /* capacity */);

			BigdataURI pred = vf
					.createURI("http://blazegraph.com/Position#hasMarketValue");
			BigdataValue obj = vf.createLiteral("100.00");

			// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E
			// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_TaxCost
			// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_UnrealizedGain
			// http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_WashSale

			final BigdataURI[] uris = new BigdataURI[] {
					vf.createURI("http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_TaxCost"),
					vf.createURI("http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_UnrealizedGain"),
					vf.createURI("http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E"),
					vf.createURI("http://blazegraph.com/Data#Position_010072F0000038090100000000D56C9E_WashSale") };

			final String[] localNames = new String[] {
					"Position_010072F0000038090100000000D56C9E_TaxCost",
					"Position_010072F0000038090100000000D56C9E_UnrealizedGain",
					"Position_010072F0000038090100000000D56C9E",
					"Position_010072F0000038090100000000D56C9E_WashSale" };


			for (int i = 0; i < uris.length; i++) {
				sb.add(uris[i], pred, obj);
			}

			sb.flush();
			store.commit();

			if (log.isDebugEnabled())
				log.debug(store.dumpStore());

			for (int i = 0; i < uris.length; i++) {

				final BigdataURI uri = uris[i];

				if (log.isDebugEnabled()) {
					log.debug("Checking " + uri.getNamespace() + " "
							+ uri.getLocalName() + " inline: "
							+ uri.getIV().isInline());
					log.debug(localNames[i] + " : " + uri.getLocalName());
				}

				//Check it is inlined
				assertTrue(uri.getIV().isInline());

				//Check the local names are correct
				assertTrue(localNames[i].equals(uri.getLocalName()));
			}

		} finally {
			store.__tearDownUnitTest();
		}

	}

	public static Test suite() {

		final TestSuite suite = new TestSuite(
				"BLZG-1938:  Single Namespace Multiple InlineURIHandlers Testing");

		suite.addTestSuite(TestMultiInlineURIHandlersSingleNamespace.class);

		return suite;

	}

}
