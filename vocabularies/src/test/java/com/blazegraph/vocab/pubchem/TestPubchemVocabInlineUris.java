package com.blazegraph.vocab.pubchem;

import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;

public class TestPubchemVocabInlineUris extends AbstractTripleStoreTestCase {

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

	public void test_PubChemInlineValues() {

		final Properties properties = getProperties();

		// Test with PubChem Vocabulary
		properties.setProperty(Options.VOCABULARY_CLASS,
				PubChemVocabulary.class.getName());

		// Test with PubChem InlineURIHandler
		properties.setProperty(Options.INLINE_URI_FACTORY_CLASS,
				PubChemInlineURIFactory.class.getName());

		// test w/o axioms - they imply a predefined vocab.
		properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());

		// test w/o the full text index.
		properties.setProperty(Options.TEXT_INDEX, "false");

		AbstractTripleStore store = getStore(properties);

		try {

			final BigdataValueFactory vf = store.getValueFactory();

			final LinkedList<BigdataURI> uriList = new LinkedList<BigdataURI>();
			
			Random rand = new Random();

			final StatementBuffer<BigdataStatement> sb = new StatementBuffer<BigdataStatement>(
					store, PubChemInlineURIFactory.uris.length/* capacity */);
			
			BigdataURI pred = vf.createURI("http://semanticscience.org/resource/CHEMINF_000461");
			BigdataURI obj = vf.createURI("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID1726184");
			
			for (int i = 0; i < PubChemInlineURIFactory.uris.length; i++) {

				final String uriStr = PubChemInlineURIFactory.uris[i];
				final String prefix = PubChemInlineURIFactory.localNames[i];				

				final int test = rand.nextInt(999999);

				final BigdataURI uri = vf.createURI(uriStr+prefix+test);

				uriList.push(uri);
				sb.add(uri, pred, obj);
			}
			
			for (int i = 0; i < PubChemInlineURIFactory.descriptorSuffix.length; i++) {

				final String uriStr = PubChemInlineURIFactory.descriptorNS;
				final String prefix = PubChemInlineURIFactory.descriptorPrefix;
				final String suffix = PubChemInlineURIFactory.descriptorSuffix[i];				

				final int test = rand.nextInt(999999);

				final BigdataURI uri = vf.createURI(uriStr+prefix+test+suffix);

				uriList.push(uri);
				sb.add(uri, pred, obj);
			}
			
			{  //Add the fixed width ones
				
				 // http://www.bioassayontology.org/bao#BAO_0002877 fixed width 7
				BigdataURI uri = null;
				uri = vf.createURI("http://www.bioassayontology.org/bao#BAO_002877");
				uriList.push(uri);
				sb.add(uri, pred, obj);

				// http://purl.obolibrary.org/obo/PR_000005253 //fixed width 9
				uri = vf.createURI("http://purl.obolibrary.org/obo/PR_000005253");
				uriList.push(uri);
				sb.add(uri, pred, obj);

				// http://purl.obolibrary.org/obo/IAO_0000136 //fixed width 7
				uri = vf.createURI("http://purl.obolibrary.org/obo/IAO_0000136");
				uriList.push(uri);
				sb.add(uri, pred, obj);

				// http://purl.obolibrary.org/obo/OBI_0000299 //fixed width 7
				uri = vf.createURI("http://purl.obolibrary.org/obo/OBI_0000299");
				uriList.push(uri);
				sb.add(uri, pred, obj);
				
				//http://purl.obolibrary.org/obo/CHEBI_74763
				uri = vf.createURI("http://purl.obolibrary.org/obo/CHEBI_74763");
				uriList.push(uri);
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

        final TestSuite suite = new TestSuite("PubChemVocabulary Inline URI Testing");

        suite.addTestSuite(TestPubchemVocabInlineUris.class);
        
        return suite;
        
	}

}