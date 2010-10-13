/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
package com.bigdata.rdf.sail.tck;

import java.util.Properties;
import java.util.UUID;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.log4j.Logger;
import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.dataset.DatasetRepository;

import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * Runs the SPARQL test suite against a {@link JiniFederation}, which must be
 * already deployed. Each test in the suite is run against a distinct quad store
 * in its own bigdata namespace.
 * 
 * @author <a href="mailto:dmacgbr@users.sourceforge.net">David MacMillan</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataFederationSparqlTest extends SPARQLQueryTest
{

    /**
     * Skip the dataset tests for now until we can figure out what is wrong with
     * them.
     * 
     * FIXME Fix the dataset tests. There is some problem in how the data to be
     * loaded into the fixture is being resolved in these tests.
     */
    public static Test suite() throws Exception {
        
        return suite(true /*hideDatasetTests*/);
        
    }
    
    public static Test suite(final boolean hideDatasetTests) throws Exception {
        
        TestSuite suite1 = fullSuite();

        // Only run the specified tests?
        if (!BigdataSparqlTest.testURIs.isEmpty()) {
            final TestSuite suite = new TestSuite();
            for (String s : BigdataSparqlTest.testURIs) {
                suite.addTest(BigdataSparqlTest.getSingleTest(suite1, s));
            }
            return suite;
        }
        
        if(hideDatasetTests)
            suite1 = BigdataSparqlTest.filterOutDataSetTests(suite1);
        
        return suite1;
        
    }

    /**
     * Return the entire test suite. 
     */
    public static TestSuite fullSuite() throws Exception {
        return ManifestTest.suite
        (
            new Factory ()
            {
                public SPARQLQueryTest createSPARQLQueryTest ( String URI, String name, String query, String results, Dataset dataSet, boolean laxCardinality)
                {
                    return new BigdataFederationSparqlTest ( URI, name, query, results, dataSet, laxCardinality ) ;
                }
            }
        ) ;
    }

    public BigdataFederationSparqlTest ( String URI, String name, String query, String results, Dataset dataSet, boolean laxCardinality )
    {
        super ( URI, name, query, results, dataSet, laxCardinality ) ;
    }

    @Override public void runTest ()
        throws Exception
    {
        _logger.info ( String.format ( ">>>>> Running test: %s", testURI ) ) ;
        super.runTest () ;
        _logger.info ( String.format ( ">>>>> Completed test: %s", testURI ) ) ;
    }

    @Override public void tearDown ()
        throws Exception
    {
        super.tearDown () ;
		/*
		 * @todo We should destroy the triple store here, but this is causing
		 * problems with tear down of the query while it is still running. Once
		 * that issue has been fixed, uncomment both the line to destroy the
		 * triple store and the line to shutdown the federation (the latter is
		 * really optional - it should be Ok to leave the federation up across
		 * the test runs, but then we will never take it down cleanly when the
		 * test suite is done. Again, that should be Ok.)
		 */
		if (_ts != null) {
			_ts.destroy();
			_ts = null;
		}
		if (_fed != null) {
			_fed.shutdownNow();
			_fed = null;
		}
	}

    @Override protected Repository newRepository ()
        throws Exception
    {
        return new DatasetRepository ( new BigdataSailRepository ( _sail = new BigdataSail ( newTripleStore () ) ) ) ;
    }

    @Override
    protected Repository createRepository() throws Exception {
        Repository repo = newRepository();
        repo.initialize();
        return repo;
    }

	protected RepositoryConnection getQueryConnection(Repository dataRep)
			throws Exception {
		// return dataRep.getConnection();
		final BigdataSailRepositoryConnection con = new BigdataSailRepositoryConnection(new BigdataSailRepository(
				_sail), _sail.getReadOnlyConnection());
//		System.err.println(_sail.getDatabase().dumpStore());
		return con;
	}

    private ScaleOutTripleStore newTripleStore ()
        throws Exception
    {
        _ts = new ScaleOutTripleStore ( getFederation (), newNamespace (), ITx.UNISOLATED, getProperties () ) ;
        _ts.create () ;
        return _ts ;
    }

    private JiniFederation<Object> getFederation ()
        throws Exception
    {
        if ( null == _fed )
        {
            JiniClient<Object> jc = new JiniClient<Object> ( new String [] { getConfiguration () } ) ;
            _fed = jc.connect () ;
        }
        return _fed ;
    }

    static private String getConfiguration ()
        throws Exception
    {
        String c = System.getProperty ( CONFIG_PROPERTY ) ;
        if ( null == c )
            throw new Exception ( String.format ( "Configuration property not set. Specify as: -D%s=<filename or URL>", CONFIG_PROPERTY ) ) ;
        return c ;
    }

    private String newNamespace ()
    {
        return "SPARQLTest_" + UUID.randomUUID ().toString () ;
    }

	/**
	 * Configuration options for the KB instances used to run the SPARQL
	 * compliance test suite.
	 * <p>
	 * Note: These properties can not be cached across tests since they have to
	 * be slightly different for some of the tests to handle things like tests
	 * which will fail with inlining enabled ot tests which require Unicode
	 * collation strength of IDENTICAL.
	 */
    private Properties getProperties () throws Exception
    {
		final Properties _properties;

		/*
		 * Specify / override some triple store properties.
		 * 
		 * Note: You must reference this object in the section for the component
		 * which will actually create the KB instance, e.g., either the
		 * RDFDataLoadMaster or the LubmGeneratorMaster.
		 */
		_properties = new Properties();

		/*
		 * Setup for quads.
		 */
		_properties.put(BigdataSail.Options.QUADS_MODE, "true");
		_properties.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		_properties.put(BigdataSail.Options.QUERY_TIME_EXPANDER, "false");

		/*
		 * The Sesame TCK forces statement level connection auto-commit so we
		 * set a flag to permit that here. However, auto-commit and this flag
		 * SHOULD NOT be used outside of the test suite as they provide an
		 * extreme performance penalty.
		 */
		// _properties.put ( BigdataSail.Options.ALLOW_AUTO_COMMIT, "true" ) ;

		/*
		 * Provide Unicode support for keys with locale-based string collation.
		 * This is more expensive in key creation during loading, but allows key
		 * comparison and sorting in the specified locale in queries.
		 * 
		 * @see com.bigdata.btree.keys.CollatorEnum
		 */
		_properties.put(KeyBuilder.Options.COLLATOR, "ICU");
		_properties.put(KeyBuilder.Options.USER_LANGUAGE, "en");
		_properties.put(KeyBuilder.Options.USER_COUNTRY, "US");
		_properties.put(KeyBuilder.Options.USER_VARIANT, "");

		/*
		 * Turn off the full text index (search for literals by keyword).
		 */
		_properties.put(BigdataSail.Options.TEXT_INDEX, "false");

		/*
		 * Turn on bloom filter for the SPO index (good up to ~2M index entries
		 * for scale-up -or- for any size index for scale-out). This is a big
		 * win for some queries on scale-out indices since we can avoid touching
		 * the disk if the bloom filter reports "false" for a key.
		 */
		_properties.put(BigdataSail.Options.BLOOM_FILTER, "true");

		/*
		 * The #of low order bits from the TERM2ID index partition local counter
		 * that will be reversed and written into the high-order bits of the
		 * term identifier. This has a strong effect on the distribution of bulk
		 * index read/write operations for the triple store. For a given value
		 * of N, a bulk write will tend to touch 2^N index partitions. Therefore
		 * if this is even roughly on the order of the number of index
		 * partitions, each bulk write will tend to be scattered to all index
		 * partitions.
		 * 
		 * Note: If this value is too large then the writes WITHIN the index
		 * partitions will become uniformly distributed, which will negatively
		 * impact index performance.
		 */
		_properties.put(BigdataSail.Options.TERMID_BITS_TO_REVERSE, "0");

		/*
		 * Option may be enabled to store blank nodes such that they are stable
		 * (they are not stored by default).
		 */
		// new NV(BigdataSail.Options.STORE_BLANK_NODES,"true");

		/*
		 * Turn inlining on or off depending on _this_ test.
		 */
		if (BigdataSparqlTest.cannotInlineTests.contains(testURI)) {
			_properties.setProperty(Options.INLINE_LITERALS, "false");
		} else {
			_properties.setProperty(Options.INLINE_LITERALS, "true");
		}

		if (BigdataSparqlTest.unicodeStrengthIdentical.contains(testURI)) {
			// Force identical Unicode comparisons.
			_properties.setProperty(Options.COLLATOR, CollatorEnum.JDK
					.toString());
			_properties.setProperty(Options.STRENGTH, StrengthEnum.Identical
					.toString());
		}

		return _properties;
    }

	/**
	 * The name of the jini configuration file for the federation.
	 */
	public static final String CONFIG_PROPERTY = "bigdata.configuration";
	
    private static final Logger _logger = Logger.getLogger ( BigdataFederationSparqlTest.class ) ;

    private static JiniFederation<Object> _fed = null ;

    private ScaleOutTripleStore _ts = null ;
    private BigdataSail _sail = null;
    
//	/**
//	 * Dumps the locators for an index of a relation.
//	 * 
//	 * @param fed
//	 * @param namespace
//	 *            The relation namespace.
//	 * @param timestamp
//	 *            The timestamp of the view.
//	 * @param keyOrder
//	 *            The index.
//	 */
//	private static void dumpMDI(AbstractScaleOutFederation<?> fed,
//			final String namespace, final long timestamp,
//			final IKeyOrder<?> keyOrder) {
//
//		final String name = namespace + "." + keyOrder.getIndexName();
//
//		final Iterator<PartitionLocator> itr = fed
//				.locatorScan(name, timestamp, new byte[] {}/* fromKey */,
//						null/* toKey */, false/* reverseScan */);
//
//		System.err.println("name=" + name + " @ "
//				+ TimestampUtility.toString(timestamp));
//		
//		while (itr.hasNext()) {
//		
//			System.err.println(itr.next());
//			
//		}
//
//	}
//	
//	public static void main(String[] args) throws ConfigurationException,
//			Exception {
//
//		String namespace = "SPARQLTest_14e097f5-eba9-4ff8-a250-a5f9f19b9c66.spo";
//		
//		long timestamp=1287001362979L;
//		
//		IKeyOrder<?> keyOrder = SPOKeyOrder.SPOC;
//		
//		JiniClient<Object> jc = new JiniClient<Object>(
//				new String[] { getConfiguration() });
//		
//		try {
//		
//			final JiniFederation<?> fed = jc.connect();
//			
//			dumpMDI(fed, namespace, timestamp, keyOrder);
//			
//		} finally {
//
//			jc.disconnect(true/* immediate */);
//			
//		}
//
//	}

}
