/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.rdf.rules;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.ProxyBigdataSailTestCase;

/**
 * Unit tests to demonstrate custom inference rules. Specify the JVM arg
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithoutSids</code> 
 * or
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithSids</code> 
 * to run this test.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */

public class TestSample extends ProxyBigdataSailTestCase {

	private static final transient Logger log = Logger.getLogger(TestSample.class);
	
	public TestSample() {
	}

	public TestSample(String arg0) {
		super(arg0);
	}

	@Override
	public Properties getProperties() {

		final Properties props = super.getProperties();

		props.setProperty(BigdataSail.Options.AXIOMS_CLASS, SampleAxioms.class.getName());
		props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, SampleVocab.class.getName());
		props.setProperty(BigdataSail.Options.CLOSURE_CLASS, SampleClosure.class.getName());
		
		props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "true");
		props.setProperty(BigdataSail.Options.JUSTIFY, "true");

		props.setProperty(BigdataSail.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "true");
		
		return props;

	}

	public void testCustomRule() throws Exception {
	
		final BigdataSail sail = getSail();

		try {
		
			sail.initialize();
			
			final BigdataSailRepository repo = new BigdataSailRepository(sail);
			
			final BigdataSailRepositoryConnection cxn
				= repo.getUnisolatedConnection();
			cxn.setAutoCommit(false);
			
			final ValueFactory vf = sail.getValueFactory();
			
			final URI a = vf.createURI(SAMPLE.NAMESPACE+"a");
			final URI b = vf.createURI(SAMPLE.NAMESPACE+"b");
			final URI c = vf.createURI(SAMPLE.NAMESPACE+"c");
			final URI thing = vf.createURI(SAMPLE.NAMESPACE+"thing");
			final URI attribute = vf.createURI(SAMPLE.NAMESPACE+"attribute");
			final URI link = vf.createURI(SAMPLE.NAMESPACE+"link");
			final URI x = vf.createURI(SAMPLE.NAMESPACE+"x");
			final URI y = vf.createURI(SAMPLE.NAMESPACE+"y");
			
			final Literal foo = vf.createLiteral("foo");
			final Literal bar = vf.createLiteral("bar");
			
			try {
				
				cxn.add(a, RDF.TYPE, thing);
				cxn.add(a, SAMPLE.SIMILAR_TO, b);
				cxn.add(a, SAMPLE.SIMILAR_TO, c);
				
				// a and b are both rdf:type #thing
				cxn.add(b, RDF.TYPE, thing);
				// so a should pick up the literal attribute "foo"
				cxn.add(b, attribute, foo);
				// but not the link to x, since x is not a literal
				cxn.add(b, link, x);
				
				// c is not the same type as a, so a should not pick up any attributes,
				// even though they are marked as similar
				cxn.add(c, attribute, bar);
				cxn.add(c, link, y);
				
				cxn.commit();
				
				if (log.isInfoEnabled()) {
					log.info("\n"+sail.getDatabase().dumpStore(true, true, false));
				}
				
				assertTrue("a should pick up foo", cxn.hasStatement(a, attribute, foo, true));
				assertFalse("a should not pick up link to x", cxn.hasStatement(a, link, x, true));
				assertFalse("a should not pick up bar", cxn.hasStatement(a, attribute, bar, true));
				assertFalse("a should not pick up link to y", cxn.hasStatement(a, link, y, true));
				
				// now test truth maintenance
				
				// removing the similarity to b will cause a to lose "foo"
				cxn.remove(a, SAMPLE.SIMILAR_TO, b);
				// adding a common type between a and c will cause a to pick up "bar"
				cxn.add(c, RDF.TYPE, thing);
				
				cxn.commit();
				
				if (log.isInfoEnabled()) {
					log.info("\n"+sail.getDatabase().dumpStore(true, true, false));
				}
				
				assertFalse("a should have lost foo", cxn.hasStatement(a, attribute, foo, true));
				assertFalse("a should not pick up link to x", cxn.hasStatement(a, link, x, true));
				assertTrue("a should have picked up bar", cxn.hasStatement(a, attribute, bar, true));
				assertFalse("a should not pick up link to y", cxn.hasStatement(a, link, y, true));
				
			} finally {
				
				cxn.close();
				
			}
			
	    } finally {
	    	
	    	sail.__tearDownUnitTest();
	    	
		}
	    
	}
	
}
	
	
