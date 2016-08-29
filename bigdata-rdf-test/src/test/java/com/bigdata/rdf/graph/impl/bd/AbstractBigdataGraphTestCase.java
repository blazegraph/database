/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.graph.impl.bd;

import java.util.Properties;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.util.IGraphFixture;
import com.bigdata.rdf.graph.util.IGraphFixtureFactory;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;

public class AbstractBigdataGraphTestCase extends AbstractGraphTestCase {

//    private static final Logger log = Logger
//            .getLogger(AbstractGraphTestCase.class);
    
    public AbstractBigdataGraphTestCase() {
    }

    public AbstractBigdataGraphTestCase(String name) {
        super(name);
    }

    @Override
    protected IGraphFixtureFactory getGraphFixtureFactory() {

        return new IGraphFixtureFactory() {
     
            @Override
            public IGraphFixture newGraphFixture() throws Exception {

                return new BigdataGraphFixture(getProperties());

            }

        };

    }

    protected Properties getProperties() {
        
        final Properties p = new Properties();

        p.setProperty(Journal.Options.BUFFER_MODE,
                BufferMode.MemStore.toString());
        
        /*
         * TODO Test both triples and quads.
         * 
         * Note: We need to use different data files for quads (trig). If we use
         * trig for a triples mode kb then we get errors (context bound, but not
         * quads mode).
         */
        p.setProperty(BigdataSail.Options.TRIPLES_MODE, "true");
//        p.setProperty(BigdataSail.Options.QUADS_MODE, "true");
        p.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        return p;
        
    }

    @Override
    protected BigdataGraphFixture getGraphFixture() {

        return (BigdataGraphFixture) super.getGraphFixture();
        
    }

    /**
     * A small foaf data set relating some of the project contributors (triples
     * mode data).
     * 
     * @see {@value #smallGraph}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class SmallGraphProblem {

        /**
         * The data file.
         */
        static private final String smallGraph = "src/test/resources/graph/data/smallGraph.ttl";

        private final BigdataURI rdfType, foafKnows, foafPerson, mike, bryan,
                martyn;

        public SmallGraphProblem() throws Exception {

            getGraphFixture().loadGraph(smallGraph);
            
            final BigdataSailConnection conn = getGraphFixture().getSail().getReadOnlyConnection();
            
            try {
                    
                final ValueFactory vf = conn.getBigdataSail().getValueFactory();
    
            rdfType = (BigdataURI) vf.createURI(RDF.TYPE.stringValue());
    
            foafKnows = (BigdataURI) vf
                    .createURI("http://xmlns.com/foaf/0.1/knows");
    
            foafPerson = (BigdataURI) vf
                    .createURI("http://xmlns.com/foaf/0.1/Person");
    
            mike = (BigdataURI) vf.createURI("http://www.bigdata.com/Mike");
    
            bryan = (BigdataURI) vf.createURI("http://www.bigdata.com/Bryan");
    
            martyn = (BigdataURI) vf.createURI("http://www.bigdata.com/Martyn");
    
            final BigdataValue[] terms = new BigdataValue[] { rdfType,
                    foafKnows, foafPerson, mike, bryan, martyn };
    
            // batch resolve existing IVs.
                conn.getTripleStore().getLexiconRelation()
                    .addTerms(terms, terms.length, true/* readOnly */);
    
            for (BigdataValue v : terms) {
                if (v.getIV() == null)
                    fail("Did not resolve: " + v);
            }

            } finally {
                
                conn.close();
                
            }

        }

        @SuppressWarnings("rawtypes")
        public IV getRdfType() {
            return rdfType.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getFoafKnows() {
            return foafKnows.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getFoafPerson() {
            return foafPerson.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getMike() {
            return mike.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getBryan() {
            return bryan.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getMartyn() {
            return martyn.getIV();
        }

    }

    /**
     * Load and setup the {@link SmallGraphProblem}.
     */
    protected SmallGraphProblem setupSmallGraphProblem() throws Exception {

        return new SmallGraphProblem();

    }

    /**
     * A small weighted graph data set.
     * 
     * @see {@value #smallWeightedGraph}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    protected class SmallWeightedGraphProblem {

        /**
         * The data file.
         */
        static private final String smallWeightedGraph = "src/test/resources/graph/data/smallWeightedGraph.ttlx";

        private final BigdataURI foafKnows, linkWeight, v1, v2, v3, v4, v5;

        public SmallWeightedGraphProblem() throws Exception {

            getGraphFixture().loadGraph(smallWeightedGraph);

            final BigdataSailConnection conn = getGraphFixture().getSail().getReadOnlyConnection();
            
            try {
                    
                final ValueFactory vf = conn.getBigdataSail().getValueFactory();
    
            foafKnows = (BigdataURI) vf
                    .createURI("http://xmlns.com/foaf/0.1/knows");
            
            linkWeight = (BigdataURI) vf
                    .createURI("http://www.bigdata.com/weight");
    
            v1 = (BigdataURI) vf.createURI("http://www.bigdata.com/1");
            v2 = (BigdataURI) vf.createURI("http://www.bigdata.com/2");
            v3 = (BigdataURI) vf.createURI("http://www.bigdata.com/3");
            v4 = (BigdataURI) vf.createURI("http://www.bigdata.com/4");
            v5 = (BigdataURI) vf.createURI("http://www.bigdata.com/5");
    
            final BigdataValue[] terms = new BigdataValue[] { foafKnows,
                    linkWeight, v1, v2, v3, v4, v5 };
    
            // batch resolve existing IVs.
                conn.getTripleStore().getLexiconRelation()
                    .addTerms(terms, terms.length, true/* readOnly */);
    
            for (BigdataValue v : terms) {
                if (v.getIV() == null)
                    fail("Did not resolve: " + v);
            }
                
            } finally {
                
                conn.close();
                
            }

        }

        @SuppressWarnings("rawtypes")
        public IV getFoafKnows() {
            return foafKnows.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getLinkWeight() {
            return linkWeight.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getV1() {
            return v1.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getV2() {
            return v2.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getV3() {
            return v3.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getV4() {
            return v4.getIV();
        }

        @SuppressWarnings("rawtypes")
        public IV getV5() {
            return v5.getIV();
        }


    }

    /**
     * Load and setup the {@link SmallWeightedGraphProblem}.
     */
    protected SmallWeightedGraphProblem setupSmallWeightedGraphProblem() throws Exception {

        return new SmallWeightedGraphProblem();

    }

}
