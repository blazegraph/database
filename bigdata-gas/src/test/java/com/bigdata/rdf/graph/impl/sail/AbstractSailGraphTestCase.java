/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.sail;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.sail.Sail;

import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.util.IGraphFixture;
import com.bigdata.rdf.graph.util.IGraphFixtureFactory;

public class AbstractSailGraphTestCase extends AbstractGraphTestCase {

//    private static final Logger log = Logger
//            .getLogger(AbstractGraphTestCase.class);
    
    public AbstractSailGraphTestCase() {
    }

    public AbstractSailGraphTestCase(String name) {
        super(name);
    }

    @Override
    protected IGraphFixtureFactory getGraphFixtureFactory() {

        return new IGraphFixtureFactory() {

            @Override
            public IGraphFixture newGraphFixture() throws Exception {
                return new SailGraphFixture();
            }
            
        };

    }

    @Override
    protected SailGraphFixture getGraphFixture() {

        return (SailGraphFixture) super.getGraphFixture();

    }

    /**
     * A small foaf data set relating some of the project contributors (triples
     * mode data).
     * 
     * @see {@value #smallGraph}
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static protected class SmallGraphProblem {

        /**
         * The data file.
         */
        static private final String smallGraph1 = "bigdata-gas/src/test/resources/com/bigdata/rdf/graph/data/smallGraph.ttl";
        static private final String smallGraph2 = "src/test/resources/com/bigdata/rdf/graph/data/smallGraph.ttl";
        
        private final URI rdfType, foafKnows, foafPerson, mike, bryan, martyn, dc;

        private final Set<Value> vertices;

        private final Set<Value> linkTypes;
        
        public Set<Value> getVertices() {
            return Collections.unmodifiableSet(vertices);
        }
        
        public Set<Value> getLinkTypes() {
            return Collections.unmodifiableSet(linkTypes);
        }
        
        public SmallGraphProblem(final SailGraphFixture graphFixture) throws Exception {

            try {
                // in eclipse with bigdata as the root dir.
                graphFixture.loadGraph(smallGraph1);
            } catch (FileNotFoundException ex) {
                // from the ant build file with bigdata-gas as the root dir.
                graphFixture.loadGraph(smallGraph2);
            }

            final Sail sail = graphFixture.getSail();

            rdfType = sail.getValueFactory().createURI(RDF.TYPE.stringValue());

            foafKnows = sail.getValueFactory().createURI(
                    "http://xmlns.com/foaf/0.1/knows");

            foafPerson = sail.getValueFactory().createURI(
                    "http://xmlns.com/foaf/0.1/Person");

            mike = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Mike");

            bryan = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Bryan");

            martyn = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/Martyn");

            dc = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/DC");

            vertices = new LinkedHashSet<Value>(Arrays.asList(new Value[] {
                    foafPerson, mike, bryan, martyn, dc }));

            linkTypes = new LinkedHashSet<Value>(Arrays.asList(new Value[] {
                    rdfType, foafKnows }));            

        }

        public URI getRdfType() {
            return rdfType;
        }

        public URI getFoafKnows() {
            return foafKnows;
        }

        public URI getFoafPerson() {
            return foafPerson;
        }

        public URI getMike() {
            return mike;
        }

        public URI getBryan() {
            return bryan;
        }

        public URI getMartyn() {
            return martyn;
        }

        public URI getDC() {
            return dc;
        }

    }

    /**
     * Load and setup the {@link SmallGraphProblem}.
     */
    protected SmallGraphProblem setupSmallGraphProblem() throws Exception {

        return new SmallGraphProblem(getGraphFixture());

    }

    /**
     * A small data set designed to demonstrate the push style scatter for SSSP.
     * 
     * <pre>
     * Source, Target, Weight
     * --------------------------------
     * 1 2 1.00
     * 1 3 1.00
     * 2 4 0.50
     * 3 4 1.00
     * 3 5 1.00
     * 4 5 0.25
     * 
     * Frontier @ t0 = {1}.   Vertices={1:0}
     * Frontier @ t1 = {2,3}. Vertices={1:0, 2:1, 3:1}.
     * Frontier @ t2 = {4,5}. Vertices={1:0, 2:1, 3:1, 4:1.5, 5:2}
     * Frontier @ t3 = {5}.   Vertices={1:0, 2:1, 3:1, 4:1.5, 5:1.75}
     * </pre>
     * 
     * @see <a href="../data/ssspGraph.ttl>ssspGraph.ttl</a>
     * @see <a href="../data/ssspGraph.png>ssspGraph.png</a>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    protected static class SSSPGraphProblem {

        /**
         * The data file.
         */
        static private final String ssspGraph1 = "src/test/resources/com/bigdata/rdf/graph/data/ssspGraph.ttlx";
        static private final String ssspGraph2 = "src/test/resources/com/bigdata/rdf/graph/data/ssspGraph.ttlx";
        
        public final URI link, v1, v2, v3, v4, v5;

        private final Set<Value> vertices;
        
        private final Set<Value> linkTypes;
        
        public Set<Value> getVertices() {
            return Collections.unmodifiableSet(vertices);
        }
        
        public Set<Value> getLinkTypes() {
            return Collections.unmodifiableSet(linkTypes);
        }
        
        public SSSPGraphProblem(final SailGraphFixture graphFixture)
                throws Exception {

            try {
                // in eclipse with bigdata as the root dir.
                graphFixture.loadGraph(ssspGraph1);
            } catch (FileNotFoundException ex) {
                // from the ant build file with bigdata-gas as the root dir.
                graphFixture.loadGraph(ssspGraph2);
            }

            final Sail sail = graphFixture.getSail();

            link = sail.getValueFactory().createURI(
                    "http://www.bigdata.com/ssspGraph/link");

            v1 = sail.getValueFactory().createURI("http://www.bigdata.com/ssspGraph/1");
            v2 = sail.getValueFactory().createURI("http://www.bigdata.com/ssspGraph/2");
            v3 = sail.getValueFactory().createURI("http://www.bigdata.com/ssspGraph/3");
            v4 = sail.getValueFactory().createURI("http://www.bigdata.com/ssspGraph/4");
            v5 = sail.getValueFactory().createURI("http://www.bigdata.com/ssspGraph/5");

            vertices = new LinkedHashSet<Value>(Arrays.asList(new Value[] {
                    v1, v2, v3, v4, v5 }));

            linkTypes = new LinkedHashSet<Value>(
                    Arrays.asList(new Value[] { link }));

        }
        
        public Value get_v1() {
            return v1;
        }
        public Value get_v2() {
            return v2;
        }
        public Value get_v3() {
            return v3;
        }
        public Value get_v4() {
            return v4;
        }
        public Value get_v5() {
            return v5;
        }

    }

    /**
     * Load and setup the {@link SSSPGraphProblem}.
     */
    protected SSSPGraphProblem setupSSSPGraphProblem() throws Exception {

        return new SSSPGraphProblem(getGraphFixture());

    }

    public void testNoop()
    {
	//FIXME:  Write some test.
	assert(true);
    }

}
