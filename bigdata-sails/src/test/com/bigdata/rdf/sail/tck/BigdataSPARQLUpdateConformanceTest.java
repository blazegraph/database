/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2012.
 *
 * Licensed under the Aduna BSD-style license.
 */
package com.bigdata.rdf.sail.tck;

import java.util.Map;
import java.util.Properties;

import junit.framework.Test;

import org.openrdf.model.URI;
import org.openrdf.query.parser.sparql.SPARQL11ManifestTest;
import org.openrdf.query.parser.sparql.SPARQLUpdateConformanceTest;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.contextaware.ContextAwareRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Harness for running the SPARQL 1.1 UPDATE compliance tests (DAWG).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataSPARQLUpdateConformanceTest extends
        SPARQLUpdateConformanceTest {

    public BigdataSPARQLUpdateConformanceTest(String testURI, String name,
            String requestFile, URI defaultGraphURI,
            Map<String, URI> inputNamedGraphs, URI resultDefaultGraphURI,
            Map<String, URI> resultNamedGraphs) {

        super(testURI, name, requestFile, defaultGraphURI, inputNamedGraphs,
                resultDefaultGraphURI, resultNamedGraphs);
        
    }

    public static Test suite() throws Exception {
        final Test suite = SPARQL11ManifestTest.suite(new Factory() {

            public BigdataSPARQLUpdateConformanceTest createSPARQLUpdateConformanceTest(
                    String testURI, String name, String requestFile,
                    URI defaultGraphURI, Map<String, URI> inputNamedGraphs,
                    URI resultDefaultGraphURI,
                    Map<String, URI> resultNamedGraphs) {
                System.err.println(">>>>> "+testURI);// FIXME REMOVE.
                return new BigdataSPARQLUpdateConformanceTest(testURI, name,
                        requestFile, defaultGraphURI, inputNamedGraphs,
                        resultDefaultGraphURI, resultNamedGraphs);
            }

        });
        
        return suite;
    }

    /**
     * Note: This method may be overridden in order to run the test suite
     * against other variations of the bigdata backend.
     */
    protected Properties getProperties() {

        final Properties props = new Properties();
        
//        final File journal = BigdataStoreTest.createTempFile();
//        
//        props.setProperty(BigdataSail.Options.FILE, journal.getAbsolutePath());

        props.setProperty(Options.BUFFER_MODE, BufferMode.Transient.toString());
        
        // quads mode: quads=true, sids=false, axioms=NoAxioms, vocab=NoVocabulary
        props.setProperty(Options.QUADS_MODE, "true");

        // no justifications
        props.setProperty(Options.JUSTIFY, "false");
        
        // no query time inference
        props.setProperty(Options.QUERY_TIME_EXPANDER, "false");
        
//        // auto-commit only there for TCK
//        props.setProperty(Options.ALLOW_AUTO_COMMIT, "true");
        
        // exact size only there for TCK
        props.setProperty(Options.EXACT_SIZE, "true");
        
//        props.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());
        
//      Force identical unicode comparisons (assuming default COLLATOR setting).
//        props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
        
        /*
         * disable read/write transactions since this class runs against the
         * unisolated connection.
         */
        props.setProperty(Options.ISOLATABLE_INDICES, "false");
        
        // disable truth maintenance in the SAIL
        props.setProperty(Options.TRUTH_MAINTENANCE, "false");
        
        return props;
        
    }
    
    @Override
    protected ContextAwareRepository newRepository() throws RepositoryException {

        if (true) {
            final Properties props = getProperties();
            
//            if (cannotInlineTests.contains(testURI)){
//                // The test can not be run using XSD inlining.
//                props.setProperty(Options.INLINE_XSD_DATATYPE_LITERALS, "false");
//                props.setProperty(Options.INLINE_DATE_TIMES, "false");
//            }
//            
//            if(unicodeStrengthIdentical.contains(testURI)) {
//                // Force identical Unicode comparisons.
//                props.setProperty(Options.COLLATOR, CollatorEnum.JDK.toString());
//                props.setProperty(Options.STRENGTH, StrengthEnum.Identical.toString());
//            }
            
            final BigdataSail sail = new BigdataSail(props);
            return new ContextAwareRepository(new BigdataSailRepository(sail));
        } else {
            /*
             * Run against openrdf.
             */
            SailRepository repo = new SailRepository(new MemoryStore());

            return new ContextAwareRepository(repo);
        }
    }

}
