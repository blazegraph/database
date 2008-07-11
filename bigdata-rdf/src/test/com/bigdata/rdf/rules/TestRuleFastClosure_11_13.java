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
 * Created on Oct 22, 2007
 */

package com.bigdata.rdf.rules;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.rules.AbstractRuleFastClosure_11_13;
import com.bigdata.rdf.rules.RDFSVocabulary;
import com.bigdata.rdf.rules.RuleFastClosure11;
import com.bigdata.rdf.rules.RuleFastClosure13;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for {@link AbstractRuleFastClosure_11_13}.
 * 
 * @see RuleFastClosure11
 * @see RuleFastClosure13
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleFastClosure_11_13 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleFastClosure_11_13() {
    }

    /**
     * @param name
     */
    public TestRuleFastClosure_11_13(String name) {
        super(name);
    }

    /**
     * Tests {@link RuleFastClosure11} with the minimum data required to compute
     * a single entailment.
     */
    public void test_RuleFastForwardClosure11() throws Exception {
        
        AbstractTripleStore store = getStore();

        try {

            final long a = store.addTerm(new _URI("http://www.bigdata.com/a"));
            final long b = store.addTerm(new _URI("http://www.bigdata.com/b"));
            final long y = store.addTerm(new _URI("http://www.bigdata.com/y"));
            final long x = store.addTerm(new _URI("http://www.bigdata.com/x"));
            final long z = store.addTerm(new _URI("http://www.bigdata.com/z"));

            RDFSVocabulary inf = new RDFSVocabulary(store);

            // told:
            {

                SPO[] told = new SPO[] {
                        //
                        new SPO(x, y, z, StatementEnum.Explicit),
                        //
                        new SPO(y, inf.rdfsSubPropertyOf.get(), a,
                                StatementEnum.Explicit),
                        //
                        new SPO(a, inf.rdfsDomain.get(), b,
                                StatementEnum.Explicit) };

                store.addStatements(told, told.length);

            }

            // entails:
            // store.addStatement(x, inf.rdfType.get(), b);

            store.commit();

            System.err.println(store.dumpStore());

            /*
             * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
             * (?x, rdf:type, ?b).
             */
            RuleFastClosure11 rule = new RuleFastClosure11(store
                    .getSPORelation().getResourceIdentifier(), inf);

            /*
             * Test run the rule.
             */

            applyRule(store, rule, 1/*solutionCount*/, 1/*mutationCount*/);
            
            // check entailments.
            assertTrue(store.hasStatement(x, inf.rdfType.get(), b));

            store.commit();

        } finally {

            store.closeAndDelete();

        }
        
    }
    
    /**
     * Tests {@link RuleFastClosure13} with the minimum data required to compute
     * a single entailment.
     * @throws Exception 
     */
    public void test_RuleFastForwardClosure13() throws Exception {
        
        AbstractTripleStore store = getStore();

        try {

            final long a = store.addTerm(new _URI("http://www.bigdata.com/a"));
            final long b = store.addTerm(new _URI("http://www.bigdata.com/b"));
            final long y = store.addTerm(new _URI("http://www.bigdata.com/y"));
            final long x = store.addTerm(new _URI("http://www.bigdata.com/x"));
            final long z = store.addTerm(new _URI("http://www.bigdata.com/z"));

            RDFSVocabulary inf = new RDFSVocabulary(store);

            // told:
            {

                SPO[] told = new SPO[] {
                        //
                        new SPO(x, y, z, StatementEnum.Explicit),
                        //
                        new SPO(y, inf.rdfsSubPropertyOf.get(), a,
                                StatementEnum.Explicit),
                        //
                        new SPO(a, inf.rdfsRange.get(), b,
                                StatementEnum.Explicit) };

                store.addStatements(told, told.length);

            }

            // entails:
            // store.addStatement(z, inf.rdfType.get(), b);

            store.commit();

            System.err.println(store.dumpStore());

            /*
             * (?x, ?y, ?z), (?y, rdfs:subPropertyOf, ?a), (?a, rdfs:domain, ?b) ->
             * (?z, rdf:type, ?b).
             */
            RuleFastClosure13 rule = new RuleFastClosure13(store
                    .getSPORelation().getResourceIdentifier(), inf);

            /*
             * Test run the rule.
             */

            applyRule(store, rule, 1/*solutionCount*/, 1/*mutationCount*/);

            // check entailments.
            assertTrue(store.hasStatement(z, inf.rdfType.get(), b));

            store.commit();

        } finally {

            store.closeAndDelete();

        }
        
    }
    
}
