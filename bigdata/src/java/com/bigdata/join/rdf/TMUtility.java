/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 24, 2008
 */

package com.bigdata.join.rdf;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.join.DelegatePredicate;
import com.bigdata.join.IConstraint;
import com.bigdata.join.IPredicate;
import com.bigdata.join.IRelation;
import com.bigdata.join.IRelationName;
import com.bigdata.join.Rule;

/**
 * A utility class for performing rule re-writes for RDF truth maintenance using
 * a "focusStore" in addition to the primary database. The focusStore is
 * typically used in one of two ways: (1) incremental loading of new data into
 * the database; and (2) collecting entailments of statements that are being
 * removed during truth maintenance.
 * <p>
 * Note: When loading a new data set into the database, the "focusStore" should
 * contain the statements that were read from the data source, e.g., some
 * RDF/XML file and the "buffer" should be configured to write on the
 * "database".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TMUtility {

    public static final transient TMUtility INSTANCE = new TMUtility();
    
    /**
     * 
     */
    private TMUtility() {
    }

    /**
     * Truth maintenance for RDF needs to compute the entailments <i>as if</i>
     * the statements in a temporary triple store (known as the focusStore) were
     * already part of the database (for insert) or as if they had already been
     * removed (for delete). Given a single rule with N predicates in the tail,
     * we generate N new rules that we will run <em>instead of</em> the
     * original rule. For each of those N new rules, we set the relation for
     * tail[i] to <i>focusIndex</i>. When the new rule is run, tail[i] will
     * read from the [focusStore] rather than the original database. All other
     * predicates in the tail will read from the fused view of the [focusStore]
     * and the [database]. As a special case, when the rule has a single
     * predicate in the tail, the predicate is only run against the
     * <i>focusStore</i> rather than database or the fused view [focusStore +
     * database]. For all cases, the head of the rule is unchanged.
     * 
     * @param rule
     *            The original rule. The {@link IPredicate}s in this
     *            {@link Rule} should identify the {@link IRelation} for the
     *            database for which truth maintenance will be performed.
     * @param focusStore
     *            The temporary database containing the statements to be added
     *            to (or removed from) the database during truth maintenance.
     * 
     * @return A list of rules constructed as specified above. When they are
     *         executed, all of the resulting rules should write on the same
     *         buffer. This has the same effect as a UNION over the entailments
     *         of the individual rules.
     */
    public List<Rule> mapRuleForTruthMaintenance(final Rule rule,
            final IRelationName<SPO> focusStore) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (focusStore == null)
            throw new IllegalArgumentException();
        
        final List<Rule> rules = new LinkedList<Rule>();

        final int tailCount = rule.getTailCount();

        /*
         * Setup the fused view (focusStore + database).
         */

        final IRelationName<SPO> fusedView;
        
        if (tailCount == 1) {

            /*
             * The fused view will not be used if there is only a single
             * predicate in the tail.
             */

            fusedView = null;

        } else {

            /*
             * Note: This assumes that the relation is the same for each of the
             * tail predicates. This is true of course for RDF, and RDF is where
             * we use truth maintenance, but it is not true in the general case.
             */

            fusedView = new SPORelationView(rule.getTail(0).getRelationName(),
                    focusStore);

        }
        
        /*
         * The head of the rule is preserved unchanged. 
         */
        final IPredicate head = rule.getHead();

        /*
         * Populate an array with the same predicate instances that are found
         * in the tail of the rule.
         */
        final IPredicate[] tail = new IPredicate[tailCount];
        {
            
            final Iterator<IPredicate> itr = rule.getTail();
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                tail[i++] = itr.next();
                
            }
            
        }

        /*
         * Populate an array with the same constraint instances that are found
         * in the rule.
         */
        final IConstraint[] constraints;
        {

            final int constraintCount = rule.getConstraintCount();

            if (constraintCount > 0) {

                constraints = new IConstraint[constraintCount];

                final Iterator<IConstraint> itr = rule.getConstraints();

                int i = 0;

                while (itr.hasNext()) {

                    constraints[i++] = itr.next();

                }

            } else {

                constraints = null;

            }
            
        }
        
        /*
         * For each predicate in the tail, create a new rule in which that
         * predicate only will read from the focusStore rather than the original
         * relation.
         */
        for (int i = 0; i < tailCount; i++) {

            // new array of predicates for the new rule's tail.
            final IPredicate[] tail2 = new IPredicate[tailCount];
            
            for (int j = 0; j < tailCount; j++) {

                final IPredicate<SPO> p = tail[j];
                final IPredicate<SPO> p2;

                if (i == j || tailCount == 1) {

                    /*
                     * Override the [ith] predicate in the tail so that it reads
                     * only from the focusStore.
                     * 
                     * Note: This is also done if there is only one predicate in
                     * the tail.
                     */

                    p2 = new DelegatePredicate<SPO>(p) {

                        public IRelationName<SPO> getRelationName() {

                            return focusStore;

                        }

                    };
                    
                } else {

                    /*
                     * Override the predicate so that it reads from the fused
                     * view of the focusStore and the database.
                     */

                    p2 = new DelegatePredicate<SPO>(p) {

                        public IRelationName<SPO> getRelationName() {

                            return fusedView;

                        }

                    };

                }

                // save a reference to the modified predicate.
                tail2[j] = p2;

            } // next tail.

            final Rule newRule = new Rule(rule.getName() + "[" + i + "]", head,
                    tail2, constraints);

            rules.add(newRule);

        }

        return rules;

    }
    
}
