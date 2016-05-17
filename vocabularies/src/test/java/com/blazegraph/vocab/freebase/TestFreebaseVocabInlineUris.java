package com.blazegraph.vocab.freebase;

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

public class TestFreebaseVocabInlineUris extends AbstractTripleStoreTestCase {

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

		// Test with Freebase Vocabulary
		properties.setProperty(Options.VOCABULARY_CLASS,
				FreebaseVocabularyFull.class.getName());

		// Test with Freebase InlineURIHandler
		properties.setProperty(Options.INLINE_URI_FACTORY_CLASS,
				FreebaseInlineUriFactory.class.getName());

		// test w/o axioms - they imply a predefined vocab.
		properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
		
		//Enable inline text literals for length less than 10
		properties.setProperty(Options.INLINE_TEXT_LITERALS,"true");
		properties.setProperty(Options.MAX_INLINE_TEXT_LENGTH, "10");
		
		//com.bigdata.rdf.store.AbstractTripleStore.inlineTextLiterals=true
		//com.bigdata.rdf.store.AbstractTripleStore.maxInlineTextLength=10

		// test w/o the full text index.
		properties.setProperty(Options.TEXT_INDEX, "false");

		return getStore(properties);
		
	}

	public void test_FreebaseInlineValues() {

		AbstractTripleStore store = getInlineProps();
		
		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final LinkedList<BigdataURI> uriList = new LinkedList<BigdataURI>();
			
			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, 2/* capacity */);
		
			//<http://rdf.freebase.com/ns/award.award_winner>	<http://rdf.freebase.com/ns/type.type.instance>
			BigdataURI pred = vf.createURI("http://rdf.freebase.com/ns/award.award_winner");
			BigdataURI obj = vf.createURI("http://rdf.freebase.com/ns/type.type.instance");
		
			{
				BigdataURI uri = vf.createURI("http://rdf.freebase.com/ns/g.121k2cfp");
				uriList.add(uri);
				sb.add(uri, pred, obj);
			}
			
			{
				BigdataURI uri = vf.createURI("http://rdf.freebase.com/ns/m.0_zdd15");
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

	public void test_FreebaseNonInlineValues() {

		AbstractTripleStore store = getInlineProps();
		
		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final LinkedList<BigdataURI> uriList = new LinkedList<BigdataURI>();
			
			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, 2/* capacity */);
		
			//<http://rdf.freebase.com/ns/award.award_winner>	<http://rdf.freebase.com/ns/type.type.instance>
			BigdataURI pred = vf.createURI("http://rdf.freebase.com/ns/award.award_winner");
			BigdataURI obj = vf.createURI("http://rdf.freebase.com/ns/type.type.instance");
		
			{
				//Should not be inlined
				BigdataURI uri = vf.createURI("http://rdf.freebase.com/ns/XXXXg.121k2cfp");
				uriList.add(uri);
				sb.add(uri, pred, obj);
			}
			
			{
				//Should not be inlined
				BigdataURI uri = vf.createURI("http://rdf.freebase.com/ns/XXXXXm.0_zdd15");
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

				assertTrue(!uri.getIV().isInline());
			}

		} finally {
			store.__tearDownUnitTest();
		}

	}

	public void test_FreebaseInlineTextValues() {

		AbstractTripleStore store = getInlineProps();
		
		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final LinkedList<BigdataLiteral> objList = new LinkedList<BigdataLiteral>();
			
			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, 2/* capacity */);
		
			//<http://rdf.freebase.com/ns/award.award_winner>	<http://rdf.freebase.com/ns/type.type.instance>
			BigdataURI uri = vf.createURI("http://rdf.freebase.com/ns/g.121k2cfp");
			BigdataURI pred = vf.createURI("http://rdf.freebase.com/ns/award.award_winner");
		
			{
				//Should be inlined
				BigdataLiteral obj = vf.createLiteral("123456789");
				objList.add(obj);
				sb.add(uri, pred, obj);
			}
			
			{
				//Should be inlined
				BigdataLiteral obj = vf.createLiteral("12345");
				objList.add(obj);
				sb.add(uri, pred, obj);
			}
			
			sb.flush();
			store.commit();

			if (log.isDebugEnabled())
				log.debug(store.dumpStore());
			
			for (final BigdataLiteral obj: objList ) {

				if(log.isDebugEnabled()) {
					log.debug("Checking  inline: " + obj.getIV().isInline());
				}

				assertTrue(obj.getIV().isInline());
			}

		} finally {
			store.__tearDownUnitTest();
		}

	}

	public void test_FreebaseNonInlineTextValues() {

		AbstractTripleStore store = getInlineProps();
		
		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final LinkedList<BigdataLiteral> objList = new LinkedList<BigdataLiteral>();
			
			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, 2/* capacity */);
		
			//<http://rdf.freebase.com/ns/award.award_winner>	<http://rdf.freebase.com/ns/type.type.instance>
			BigdataURI uri = vf.createURI("http://rdf.freebase.com/ns/g.121k2cfp");
			BigdataURI pred = vf.createURI("http://rdf.freebase.com/ns/award.award_winner");
		
			{
				//Should not be inlined
				BigdataLiteral obj = vf.createLiteral("12345678912");
				objList.add(obj);
				sb.add(uri, pred, obj);
			}
			
			{
				//Should not be inlined
				BigdataLiteral obj = vf.createLiteral("123456789123456789");
				objList.add(obj);
				sb.add(uri, pred, obj);
			}
			
			sb.flush();
			store.commit();

			if (log.isDebugEnabled())
				log.debug(store.dumpStore());
			
			for (final BigdataLiteral obj: objList ) {

				if(log.isDebugEnabled()) {
					log.debug("Checking  inline: " + obj.getIV().isInline());
				}

				assertTrue(!obj.getIV().isInline());
			}

		} finally {
			store.__tearDownUnitTest();
		}

	}
	
	public static Test suite() {

        final TestSuite suite = new TestSuite("FreebaseVocabulary Inline URI Testing");

        suite.addTestSuite(TestFreebaseVocabInlineUris.class);
        
        return suite;
        
	}

}