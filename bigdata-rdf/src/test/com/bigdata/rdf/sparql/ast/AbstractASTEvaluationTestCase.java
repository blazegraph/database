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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.engine.AbstractQueryEngineTestCase;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AbstractASTEvaluationTestCase extends AbstractQueryEngineTestCase {

    protected static final Logger log = Logger
            .getLogger(AbstractASTEvaluationTestCase.class);

    /**
     * 
     */
    public AbstractASTEvaluationTestCase() {
    }

    public AbstractASTEvaluationTestCase(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        
        super.setUp();
        
        store = getStore(getProperties());
        
    }
    
    protected void tearDown() throws Exception {
        
        if (store != null) {
        
            store.__tearDownUnitTest();
            
            store = null;
        
        }
        
        super.tearDown();
        
    }

    protected AbstractTripleStore store = null;

    public Properties getProperties() {

        // Note: clone to avoid modifying!!!
        final Properties properties = (Properties) super.getProperties().clone();

        // turn on quads.
        properties.setProperty(AbstractTripleStore.Options.QUADS, "true");

//        // override the default vocabulary.
//        properties.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS,
//                NoVocabulary.class.getName());

        // turn off axioms.
        properties.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS,
                NoAxioms.class.getName());

        // no persistence.
        properties.setProperty(com.bigdata.journal.Options.BUFFER_MODE,
                BufferMode.Transient.toString());
        
        return properties;

    }

    protected AbstractTripleStore getStore(final Properties properties) {

        final String namespace = "kb";

        // create/re-open journal.
        final Journal journal = new Journal(properties);

        final LocalTripleStore lts = new LocalTripleStore(journal, namespace,
                ITx.UNISOLATED, properties);

        lts.create();

        return lts;

    }

    static protected void assertSameAST(final IQueryNode expected,
            final IQueryNode actual) {

        if (!expected.equals(actual)) {

            log.error("expected: " + BOpUtility.toString((BOp) expected));
            log.error("actual  : " + BOpUtility.toString((BOp) actual));

            diff((BOp) expected, (BOp) actual);

            // No difference was detected?
            throw new AssertionError();
//            fail("expected:\n" + BOpUtility.toString((BOp) expected)
//                    + "\nactual:\n" + BOpUtility.toString((BOp) actual));

        }

    }

    /**
     * Throw an exception for the first operator having a ground difference
     * (different Class, different arity, or different annotation). When both
     * operators have the same named annotation but the annotation values differ
     * and they are both bop valued annotations, then the difference will be
     * reported for the annotation bops.
     * 
     * @param sb
     * @param o1
     * @param o2
     */
    static protected void diff(final BOp o1, final BOp o2) {

        if (log.isDebugEnabled())
            log.debug("Comparing: "
                + (o1 == null ? "null" : o1.toShortString()) + " with "
                + (o2 == null ? "null" : o2.toShortString()));
        
        if (o1 == o2) // same ref, including null.
            return;

        if (o1 == null && o2 != null) {

            fail("Expecting null, but have " + o2);

        }
        
        if (!o1.getClass().equals(o2.getClass())) {

            fail("Types differ: expecting " + o1.getClass() + ", but have "
                    + o2.getClass() + " for " + o1.toShortString() + ", "
                    + o2.toShortString() + "\n");

        }

        final int arity1 = o1.arity();

        final int arity2 = o2.arity();
        
        if (arity1 != arity2) {
         
            fail("Arity differs: expecting " + arity1 + ", but have " + arity2
                    + " for " + o1.toShortString() + ", " + o2.toShortString()
                    + "\n");
            
        }

        for (int i = 0; i < arity1; i++) {

            final BOp c1 = o1.get(i);
            final BOp c2 = o2.get(i);

            diff(c1, c2);

        }

        for (String name : o1.annotations().keySet()) {

            final Object a1 = o1.getProperty(name);

            final Object a2 = o2.getProperty(name);

            if (log.isDebugEnabled())
                log.debug("Comparing: "
                    + o1.getClass().getSimpleName()
                    + " @ \""
                    + name
                    + "\" having "
                    + (a1 == null ? "null" : (a1 instanceof BOp ? ((BOp) a1)
                            .toShortString() : a1.toString()))//
                    + " with "
                    + //
                    (a2 == null ? "null" : (a2 instanceof BOp ? ((BOp) a2)
                            .toShortString() : a2.toString()))//
            );

            if (a1 == a2) // same ref, including null.
                continue;

            if (a1 == null && a2 != null) {
                
                fail("Not expecting annotation for " + name + " : expecting="
                        + o1 + ", actual=" + o2);

            }

            if (a2 == null) {

                fail("Missing annotation @ \"" + name + "\" : expecting=" + o1
                        + ", actual=" + o2);

            }

            if (a1 instanceof BOp && a2 instanceof BOp) {

                // Compare BOPs in depth.
                diff((BOp) a1, (BOp) a2);

            } else {

                if (!a1.equals(a2)) {

                    fail("Annotations differ for " + name + "  : expecting="
                            + o1 + ", actual=" + o2);

                }

            }

        }
        
        final int n1 = o1.annotations().size();
        
        final int n2 = o2.annotations().size();

        if (n1 != n2) {

            fail("#of annotations differs: expecting " + o1 + ", actual=" + o2);

        }

        if(o1 instanceof IVariableOrConstant<?>) {
            
            /*
             * Note: Var and Constant both have a piece of non-BOp data which is
             * their name (Var) and their Value (Constant). The only way to
             * check those things is by casting or using Var.equals() or
             * Constant.equals().
             */

            if (!o1.equals(o2)) {

                // A difference in the non-BOp value of the variable or
                // constant.

                fail("Expecting: " + o1 + ", actual=" + o2);
                
            }
            
        }
        
        if (!o1.equals(o2)) {

//            o1.equals(o2); // FIXME Remove debug point.
            
            fail("Failed to detect difference reported by equals(): expected="
                    + o1 + ", actual=" + o2);
            
        }
        
    }

}
