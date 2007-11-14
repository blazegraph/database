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
 * Created on Apr 18, 2007
 */

package com.bigdata.rdf.inf;

import com.bigdata.rdf.inf.Rule.State;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleTestCase extends AbstractInferenceEngineTestCase {
    
    /**
     * 
     */
    public AbstractRuleTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRuleTestCase(String name) {
        super(name);
    }

    /**
     * Applies the rule, copies the new entailments into the store and checks
     * the expected #of inferences computed and new statements copied into the
     * store.
     * <p>
     * Invoke as <code>applyRule( store.{rule}, ..., ... )</code>
     * 
     * @param rule
     *            The rule, which must be one of those found on {@link #store}
     *            or otherwise configured so as to run with the {@link #store}
     *            instance.
     * 
     * @param expectedComputed
     *            The #of entailments that should be computed by the rule.
     * 
     * @todo setup to write entailments on the database and without using a
     *       "focusStore".
     * 
     * @deprecated this causes problems when we eagerly write the axioms on the
     *             database since there is a dependency on the inference engine
     *             which makes it much harder to test the rules in a closed
     *             world scenarior.
     */
    protected RuleStats applyRule(InferenceEngine inf,Rule rule, int expectedComputed) {
        
        /*
         * Note: Choose a capacity large enough that all entailments will still
         * be in the buffer until we explicitly flush them to the store. This
         * let's us dump the entailments to the console below.
         */
        
        final int capacity = Math.max(expectedComputed, 1000);
        
        AbstractTripleStore db = inf.database;
        
        SPOAssertionBuffer buffer = new SPOAssertionBuffer( db,
                inf.doNotAddFilter/* filter */, capacity, inf.isJustified());
        
        // dump the database on the console.
        System.err.println("database::");
        db.dumpStore();
        
        State state = rule.newState(inf.isJustified(), db, buffer);
        
        // apply the rule.
        rule.apply(state);
        
        // dump entailments on the console.
        System.err.println("entailments:: (may duplicate statements in the database)");
        buffer.dump(db/*used to resolve term identifiers*/);

        // flush entailments to the database.
        final int nwritten = buffer.flush();
        
        System.err.println("after write on the database");
        db.dumpStore();

        /*
         * Verify the #of entailments computed. 
         */
        if(expectedComputed!=-1) {

            /*
             * This is disabled since axioms are flooded into the database and
             * the counts are therefore wrong.
             * 
             * FIXME review all unit tests since this is now disable.
             */
            
//            assertEquals("numComputed",expectedComputed,state.stats.numComputed);
            
        }
        
        return state.stats;
        
    }

    protected RuleStats applyRule(AbstractTripleStore db, Rule rule, int expectedComputed) {

        return applyRule(db, rule, null/*filter*/, false/*justified*/, expectedComputed);
        
    }
    
    protected RuleStats applyRule(AbstractTripleStore db, Rule rule,
            ISPOFilter filter, boolean justified, int expectedComputed) {
        
        /*
         * Note: Choose a capacity large enough that all entailments will still
         * be in the buffer until we explicitly flush them to the store. This
         * let's us dump the entailments to the console below.
         */
        
        final int capacity = Math.max(expectedComputed, 1000);
        
        SPOAssertionBuffer buffer = new SPOAssertionBuffer(db, filter,
                capacity, justified);
        
        // dump the database on the console.
        System.err.println("database::");
        db.dumpStore();
        
        State state = rule.newState(justified, db, buffer);
        
        // apply the rule.
        rule.apply(state);
        
        // dump entailments on the console.
        System.err.println("entailments:: (may duplicate statements in the database)");
        buffer.dump(db/*used to resolve term identifiers*/);

        // flush entailments to the database.
        final int nwritten = buffer.flush();
        
        System.err.println("after write on the database");
        db.dumpStore();

        /*
         * Verify the #of entailments computed. 
         */
        if(expectedComputed!=-1) {
    
            assertEquals("numComputed",expectedComputed,state.stats.numComputed);
            
        }
        
        return state.stats;
        
    }

}
