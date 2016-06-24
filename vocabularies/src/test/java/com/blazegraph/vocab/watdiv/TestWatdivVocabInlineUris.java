package com.blazegraph.vocab.watdiv;

import java.util.LinkedList;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

public class TestWatdivVocabInlineUris extends AbstractTripleStoreTestCase {

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
		props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");

		return props;

	}
	
	protected AbstractTripleStore getInlineProps() {
		final Properties properties = getProperties();

		// Test with Watdiv Vocabulary
		properties.setProperty(Options.VOCABULARY_CLASS,
				WatdivVocabulary.class.getName());

		// Test with Watdiv InlineURIHandler
		properties.setProperty(Options.INLINE_URI_FACTORY_CLASS,
				WatdivInlineURIFactory.class.getName());

		// test w/o axioms - they imply a predefined vocab.
		properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
		
		// test w/o the full text index.
		properties.setProperty(Options.TEXT_INDEX, "false");

		return getStore(properties);
		
	}

	public void test_WatdivInlineValues() {

		AbstractTripleStore store = getInlineProps();
		
		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final LinkedList<BigdataURI> uriList = new LinkedList<BigdataURI>();
			
			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, 2/* capacity */);
		
			BigdataURI pred = vf.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/friendOf");
			BigdataURI obj = vf.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/Genre100");
		
			for(int i = 0; i < WatdivInlineURIFactory.INLINE_INT_URIS.length; i++)
			{
				BigdataURI uri = vf.createURI(WatdivInlineURIFactory.INLINE_INT_URIS[i]+"1234");
				uriList.add(uri);
				sb.add(uri, pred, obj);
			}
			
			sb.flush();
			store.commit();

			if (log.isDebugEnabled())
				log.debug(store.dumpStore());
			
			for (final BigdataURI uri: uriList ) {

				if(log.isDebugEnabled()) {
					log.debug("Checking " + uri.getNamespace() + " "+ uri.getLocalName() + " inline: " + uri.getIV().isInline());
				}

				assertTrue(uri.getIV().isInline());
			}

		} finally {
			store.__tearDownUnitTest();
		}

	}

	
	public static Test suite() {

        final TestSuite suite = new TestSuite("WatdivVocabulary Inline URI Testing");

        suite.addTestSuite(TestWatdivVocabInlineUris.class);
        
        return suite;
        
	}

}