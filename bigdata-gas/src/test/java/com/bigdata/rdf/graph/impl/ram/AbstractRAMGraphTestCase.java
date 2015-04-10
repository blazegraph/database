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
package com.bigdata.rdf.graph.impl.ram;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.graph.AbstractGraphTestCase;
import com.bigdata.rdf.graph.impl.ram.RAMGASEngine.RAMGraph;
import com.bigdata.rdf.graph.util.IGraphFixture;
import com.bigdata.rdf.graph.util.IGraphFixtureFactory;

public class AbstractRAMGraphTestCase extends AbstractGraphTestCase {

//    private static final Logger log = Logger
//            .getLogger(AbstractGraphTestCase.class);
    
    public AbstractRAMGraphTestCase() {
    }

    public AbstractRAMGraphTestCase(String name) {
        super(name);
    }

    @Override
    protected IGraphFixtureFactory getGraphFixtureFactory() {

        return new IGraphFixtureFactory() {

            @Override
            public IGraphFixture newGraphFixture() throws Exception {
                return new RAMGraphFixture();
            }
            
        };

    }

    @Override
    protected RAMGraphFixture getGraphFixture() {

        return (RAMGraphFixture) super.getGraphFixture();

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

        private final URI rdfType, rdfsLabel, foafKnows, foafPerson, mike,
                bryan, martyn, dc;

        /**
         * The data:
         * 
         * <pre>
         * @prefix : <http://www.bigdata.com/> .
         * @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
         * @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
         * @prefix foaf: <http://xmlns.com/foaf/0.1/> .
         * 
         * #: {
         *    :Mike rdf:type foaf:Person .
         *    :Bryan rdf:type foaf:Person .
         *    :Martyn rdf:type foaf:Person .
         * 
         *    :Mike rdfs:label "Mike" .
         *    :Bryan rdfs:label "Bryan" .
         *    :DC rdfs:label "DC" .
         * 
         *    :Mike foaf:knows :Bryan .
         *    :Bryan foaf:knows :Mike .
         *    :Bryan foaf:knows :Martyn .
         *    :Martyn foaf:knows :Bryan .
         * #}
         * </pre>
         */
        public SmallGraphProblem() throws Exception {

            final RAMGraph g = getGraphFixture().getGraph();

            final ValueFactory vf = g.getValueFactory();

            rdfType = vf.createURI(RDF.TYPE.stringValue());
            rdfsLabel = vf.createURI(RDFS.LABEL.stringValue());
            foafKnows = vf.createURI("http://xmlns.com/foaf/0.1/knows");
            foafPerson = vf.createURI("http://xmlns.com/foaf/0.1/Person");
            mike = vf.createURI("http://www.bigdata.com/Mike");
            bryan = vf.createURI("http://www.bigdata.com/Bryan");
            martyn = vf.createURI("http://www.bigdata.com/Martyn");
            dc = vf.createURI("http://www.bigdata.com/DC");
            
            g.add(vf.createStatement(mike, rdfType, foafPerson));
            g.add(vf.createStatement(bryan, rdfType, foafPerson));
            g.add(vf.createStatement(martyn, rdfType, foafPerson));

            g.add(vf.createStatement(mike, rdfsLabel, vf.createLiteral("Mike")));
            g.add(vf.createStatement(bryan, rdfsLabel, vf.createLiteral("Bryan")));
            g.add(vf.createStatement(dc, rdfsLabel, vf.createLiteral("DC")));

            g.add(vf.createStatement(mike, foafKnows, bryan));
            g.add(vf.createStatement(bryan, foafKnows, mike));
            g.add(vf.createStatement(bryan, foafKnows, martyn));
            g.add(vf.createStatement(martyn, foafKnows, bryan));

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

    }

    /**
     * Load and setup the {@link SmallGraphProblem}.
     */
    protected SmallGraphProblem setupSmallGraphProblem() throws Exception {

        return new SmallGraphProblem();

    }

    public void testNoop()
    {
	assert(true);
    }

}
