/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 26, 2007
 */

package com.bigdata.rdf.rules;

import java.util.Properties;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Rule;

/**
 * Test for {@link RuleRdfs03}. Also see {@link TestRuleRdfs07}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdfs03 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdfs03() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleRdfs03(String name) {
        super(name);
    }
    
    /**
     * Literals may not appear in the subject position, but an rdfs4b entailment
     * can put them there unless you explicitly filter it out.
     * <P>
     * Note: {@link RuleRdfs04b} is the other way that literals can be entailed
     * into the subject position.
     * 
     * @throws Exception 
     */
    public void test_rdfs3_filterLiterals() throws Exception {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        try {
        
            URI A = new URIImpl("http://www.foo.org/A");
            URI X = new URIImpl("http://www.foo.org/X");
            URI U = new URIImpl("http://www.foo.org/U");
            Literal V1 = new LiteralImpl("V1"); // a literal.
            URI V2 = new URIImpl("http://www.foo.org/V2"); // not a literal.
            URI rdfRange = RDFS.RANGE;
            URI rdfType = RDF.TYPE;

            store.addStatement(A, rdfRange, X);
            store.addStatement(U, A, V1);
            store.addStatement(U, A, V2);

            assertTrue(store.hasStatement(A, rdfRange, X));
            assertTrue(store.hasStatement(U, A, V1));
            assertTrue(store.hasStatement(U, A, V2));
            assertEquals(3,store.getStatementCount());

            /*
             * Note: The rule computes the entailement but it gets whacked by
             * the DoNotAddFilter on the InferenceEngine, so it is counted here
             * but does not show in the database.
             */
            
            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleRdfs03(
                    store.getSPORelation().getNamespace(), vocab);
            
            final IElementFilter<ISPO> filter = new DoNotAddFilter(vocab,
                    store.getAxioms(), true/* forwardChainRdfTypeRdfsResource */);
            
            applyRule(store, r, filter/* , false justified */,
                    -1/* solutionCount */, 1/* mutationCount*/);

            /*
             * validate the state of the primary store.
             */
            assertTrue(store.hasStatement(A, rdfRange, X));
            assertTrue(store.hasStatement(U, A, V1));
            assertTrue(store.hasStatement(U, A, V2));
            assertTrue(store.hasStatement(V2, rdfType, X));
            assertEquals(4,store.getStatementCount());

        } finally {

            store.__tearDownUnitTest();

        }

    }

    /**
     * 
     */
    public void test_rdfs3_01() throws Exception {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        try {
        
            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI rdfsRange = RDFS.RANGE;
            URI rdfsClass= RDFS.CLASS;
            URI rdfType = RDF.TYPE;

            store.addStatement(A, rdfType, B);
            store.addStatement(rdfType, rdfsRange, rdfsClass);

            assertTrue(store.hasStatement(A, rdfType, B));
            assertTrue(store.hasStatement(rdfType, rdfsRange, rdfsClass));
            assertEquals(2,store.getStatementCount());

            final Vocabulary vocab = store.getVocabulary();
            
            final Rule r = new RuleRdfs03(
                    store.getSPORelation().getNamespace(), vocab);

            applyRule(store, r, -1/* solutionCount */, 1/* mutationCount */);

            /*
             * validate the state of the primary store.
             */
            assertTrue(store.hasStatement(A, rdfType, B));
            assertTrue(store.hasStatement(rdfType, rdfsRange, rdfsClass));
            assertTrue(store.hasStatement(B, rdfType, rdfsClass));
            assertEquals(3,store.getStatementCount());

        } finally {

            store.__tearDownUnitTest();

        }

    }

}
