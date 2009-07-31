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

package com.bigdata.rdf.iris;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.rules.TMUtility;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;

public class MagicTMUtility extends TMUtility {

    /**
     * Default instance constructs programs with parallel steps.
     */
    public static final transient MagicTMUtility INSTANCE = new MagicTMUtility(true/*parallel*/);
    
    /**
     * Alternative instance constructs programs with sequential steps for easier
     * debugging.
     */
    public static final transient MagicTMUtility DEBUG = new MagicTMUtility(false/*parallel*/);
    
    /**
     * @param parallel
     *            When <code>true</code>, the new {@link IRule}s will be
     *            added to a program that executes its steps in parallel.
     */
    public MagicTMUtility(boolean parallel) {
    
        super(parallel);
        
    }

    /**
     * See {@link TMUtility#mapRuleForTruthMaintenance(IRule, String)}.  Needs
     * to handle magic predicates (not change their read/write relation.)
     * 
     * @param rule
     *            The original rule. The {@link IPredicate}s in this
     *            {@link Rule} should identify the {@link IRelation} for the
     *            database for which truth maintenance will be performed.
     * @param focusStore
     *            The temporary database containing the statements to be added
     *            to (or removed from) the database during truth maintenance.
     * 
     * @return An {@link IProgram} constructed as specified above. When they are
     *         executed, all of the resulting rules should write on the same
     *         buffer. This has the same effect as a UNION over the entailments
     *         of the individual rules.
     */
    @Override
    public Program mapRuleForTruthMaintenance(final IRule rule,
            final String focusStore) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (focusStore == null)
            throw new IllegalArgumentException();
        
        final List<IRule> rules = new LinkedList<IRule>();

        final int tailCount = rule.getTailCount();

        /*
         * The head of the rule is modified to write on the focusStore. 
         */
        
        if (rule.getHead() == null)
            throw new IllegalArgumentException("No head for this rule: rule="
                    + rule);
        
        IPredicate head = rule.getHead();
        
        if (head instanceof SPOPredicate) {
            
            head = head.setRelationName(new String[] { focusStore });
            
        } else { 
            
            // head is a magic predicate with the relation already correctly set
            // it will write on the appropriate magic relation, wherever that
            // may be (probably in the focus store)
            
        }

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

                final IPredicate p = tail[j];
                final IPredicate p2;

                if (p instanceof SPOPredicate) {
                
                    if (i == j || tailCount == 1) {
    
                        /*
                         * Override the [ith] predicate in the tail so that it 
                         * reads only from the focusStore.
                         * 
                         * Note: This is also done if there is only one 
                         * predicate in the tail.
                         */
    
                        p2 = p.setRelationName(new String[]{focusStore});
    
                    } else {
    
                        /*
                         * Override the predicate so that it reads from the 
                         * fused view of the focusStore and the database.
                         */
    
                        p2 = p.setRelationName(new String[] {
                                p.getOnlyRelationName(), focusStore });
    
                    }
                    
                } else {
                    
                    p2 = p.copy();
                    
                    // tail is a magic predicate with the relation already correctly set
                    // it will read from the appropriate magic relation, wherever that
                    // may be (probably in the focus store)
                    
                }

                // save a reference to the modified predicate.
                tail2[j] = p2;

            } // next tail.

            final Rule newRule = new Rule(rule.getName() + "[" + i + "]", head,
                    tail2, rule.getQueryOptions(), constraints, rule
                            .getConstants(), rule.getTaskFactory());

            rules.add(newRule);

        }

        final Program program = new Program(rule.getName(), parallel);

        program.addSteps(rules.iterator());
        
        return program;

    }

}
