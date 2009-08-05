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

package com.bigdata.rdf.rules;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.Rule;

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

    /**
     * Default instance constructs programs with parallel steps.
     */
    public static final transient TMUtility INSTANCE = new TMUtility(true/*parallel*/);
    
    /**
     * Alternative instance constructs programs with sequential steps for easier
     * debugging.
     */
    public static final transient TMUtility DEBUG = new TMUtility(false/*parallel*/);
    
    protected final boolean parallel;
    
    /**
     * @param parallel
     *            When <code>true</code>, the new {@link IRule}s will be
     *            added to a program that executes its steps in parallel.
     */
    public TMUtility(boolean parallel) {
    
        this.parallel = parallel;
        
    }

    /**
     * Truth maintenance for RDF needs to compute the entailments <i>as if</i>
     * the statements in a temporary triple store (known as the focusStore) were
     * already part of the database (for insert) or as if they had already been
     * removed (for delete). Given a single rule with N predicates in the tail,
     * we generate N new rules that we will run <em>instead of</em> the
     * original rule. For each of those N new rules in turn, we set the relation
     * for tail[i] to <i>focusStore</i>. When the new rule is run, tail[i] will
     * read from the [focusStore] rather than the original database. All other
     * predicates in the tail are modified to read from the fused view of the
     * [database + focusStore]. As a special case, when the rule has a single
     * predicate in the tail, the predicate is only run against the
     * <i>focusStore</i> rather than <i>database</i> or the fused view
     * <i>[database + focusStore]. For all cases, the head of the rule is
     * unchanged - this is the relation on which the rules will write and it
     * should always be the <i>database</i>.
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

    /**
     * Map a {@link IProgram} for truth maintenance.
     * 
     * @param program
     *            The program.
     * @param focusStore
     *            The focus store.
     *            
     * @return The new program.
     * 
     * @todo unit tests.
     */
    public Program mapProgramForTruthMaintenance(IProgram program,
            String focusStore) {

        if (program == null)
            throw new IllegalArgumentException();

        if (focusStore == null)
            throw new IllegalArgumentException();

        final Program tmp = new MyProgram(program.getName(), program
                .isParallel(), program.isClosure());
        
        final Iterator<? extends IStep> itr = program.steps();
        
        while(itr.hasNext()) {
            
            tmp.addSteps(mapForTruthMaintenance(itr.next(), focusStore)
                            .steps());
        
        }
        
        return tmp;
        
    }

    /**
     * Class exposes protected ctor.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class MyProgram extends Program {

        /**
         * 
         */
        private static final long serialVersionUID = 2466870779751510938L;

        /**
         * @param name
         * @param parallel
         * @param closure
         */
        protected MyProgram(String name, boolean parallel, boolean closure) {

            super(name, parallel, closure);
            
        }
           
    }
    
    public Program mapForTruthMaintenance(IStep step, String focusStore) {

        if(step.isRule()) {
            
            return mapRuleForTruthMaintenance((IRule)step, focusStore);
            
        } else {
            
            return mapProgramForTruthMaintenance((IProgram)step, focusStore);
            
        }
        
    }

}
