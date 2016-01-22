package com.bigdata.gom;

import java.util.Properties;

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParserRegistry;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.rio.turtle.BigdataTurtleParser;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for numeric bnodes parsing.
 */
public class TestNumericBNodes extends RemoteGOMTestCase {

	@Override
	protected Properties getProperties() throws Exception {
		
		final Properties props = super.getProperties();
		
		props.setProperty(AbstractTripleStore.Options.QUADS, "true");
		props.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
		
		return props;
		
	}

//	
//	/**
//	 * Mike,
//	 * 
//	 * If you load the attached file into the NSS and then execute
//	 * bigdata-gom/samples//Example1 (or Example2) it will throw an exception
//	 * having to do with bnode Ids. This is the issue that David Booth posted
//	 * here [1].
//	 * 
//	 * I'd appreciate it if you could look at this in the AM. I am trying to get
//	 * some times to run the SPARQL queries in those sample programs and load
//	 * the data into the GOM API. These data are from a much larger data set.
//	 * They are the 1st 1000 lines from [2,3]. I would like to be able to load a
//	 * reasonable sample of those data, maybe 1 few million statements, and show
//	 * how fast it is to run the SPARQL queries.
//	 * 
//	 * Those examples both do a "friend-of-a-friend" computation, returning all
//	 * people that are friends of friends but not direct friends plus the
//	 * connection count and some (optional) labels. You can peek at the examples
//	 * to see how this is working in GPO code.
//	 * 
//	 * Martyn and I are working through some cache consistency issues. Right
//	 * now, GOM tries to materialize things from a DESCRIBE cache. That is cool,
//	 * but it is doing this even when we want to populate the GPOs directly from
//	 * a CONSTRUCT or SELECT query.
//	 * 
//	 * Thanks, Bryan
//	 * 
//	 * [1] https://sourceforge.net/apps/trac/bigdata/ticket/572 (bigdata
//	 * generates bnode Ids that it can not read). 
//	 * [2] http://sw.deri.org/2009/01/visinav/faq.html (ViziNav) 
//	 * [3] http://sw.deri.org/2009/01/visinav/current.nq.gz (TBL plus 6-degrees of
//	 * freedom)
//	 * 
//	 * In Sesame 2.7 we are no longer rolling our own NQuads parser.  If the
//	 * data is not parseable that is an issue with the Sesame parser.
//	 * 
//	 * @throws Exception
//	 */
//	public void test_nquads_01() throws Exception {
//		
////		final AbstractTripleStore store = getStore();
//
//		try {
//
//			// Verify that the correct parser will be used.
//			assertEquals("TurtleParserClass",
//					BigdataTurtleParser.class.getName(), RDFParserRegistry
//							.getInstance().get(RDFFormat.TURTLE).getParser()
//							.getClass().getName());
//
//			final String resource = "foaf-tbl-plus-6-degrees-small.nq";
//			
//			load(getClass().getResource(resource), RDFFormat.NQUADS);
//			
//			new Example1(om).call();
//			
//		} finally {
//
////			store.__tearDownUnitTest();
//
//		}
//
//	}

}
