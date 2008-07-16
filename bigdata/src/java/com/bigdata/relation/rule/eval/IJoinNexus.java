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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation.rule.eval;

import java.io.Serializable;

import com.bigdata.btree.BTree;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.IStep;

/**
 * Interface provides an interoperability nexus for the {@link IPredicate}s,
 * {@link IBindingSet}s, and {@link ISolution}s for the evaluation of an
 * {@link IRule} and is responsible for resolving the relation symbol to the
 * {@link IRelation} object. Instances of this interface may be type-specific
 * and allow you to control various implementation classes used during
 * {@link IRule} execution.
 * <p>
 * Note: This interface is NOT {@link Serializable}. Use an
 * {@link IJoinNexusFactory} to create instances of this interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJoinNexus {

    /**
     * The factory object is used to materialize appropriate {@link IJoinNexus}
     * instances when the rule execution crosses an RMI boundary.
     */
    IJoinNexusFactory getJoinNexusFactory();
    
    /**
     * The kind of operation that is being executed (Query, Insert, or Delete).
     */
    ActionEnum getAction();
    
    /**
     * Copy values the values from the visited element corresponding to the
     * given predicate into the binding set.
     * 
     * @param e
     *            An element visited for the <i>predicate</i> using some
     *            {@link IAccessPath}.
     * @param predicate
     *            The {@link IPredicate} providing the {@link IAccessPath}
     *            constraint.
     * @param bindingSet
     *            A set of bindings. The bindings for the predicate will be
     *            copied from the element and set on this {@link IBindingSet} as
     *            a side-effect.
     *            
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     */
    void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet);

    /**
     * Create a new {@link ISolution}. The behavior of this method generally
     * depends on bit flags specified when the {@link IJoinNexus} was created.
     * <p>
     * Note: For many purposes, it is only the computed {@link #ELEMENT}s that
     * are of interest. For high-level query, you will generally specify only
     * the {@link #BINDINGS}. The {@link #BINDINGS} are also useful for some
     * truth maintenance applications. The {@link #RULE} is generally only of
     * interest for inspecting the behavior of some rule set.
     * 
     * @param rule
     *            The rule.
     * @param bindingSet
     *            The bindings (the implementation MUST clone the bindings if
     *            they will be saved with the {@link ISolution}).
     * 
     * @return The new {@link ISolution}.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     * 
     * @see #ELEMENT
     * @see #BINDINGS
     * @see #RULE
     * @see #solutionFlags()
     * @see Solution
     */
    ISolution newSolution(IRule rule, IBindingSet bindingSet);

    /**
     * The flags that effect the behavior of {@link #newSolution(IRule, IBindingSet)}.
     */
    public int solutionFlags();
    
    /**
     * Bit flag indicating that {@link #newSolution(IRule, IBindingSet)} should
     * materialize an element from the {@link IRule} and {@link IBindingSet} and
     * make it available via {@link ISolution#get()}.
     */
    final int ELEMENT = 1 << 0;

    /**
     * Bit flag indicating that {@link #newSolution(IRule, IBindingSet)} should
     * clone the {@link IBindingSet} and make it available via
     * {@link ISolution#getBindingSet()}.
     */
    final int BINDINGS = 1 << 1;

    /**
     * Bit flag indicating that {@link #newSolution(IRule, IBindingSet)} make
     * the {@link IRule} that generated the {@link ISolution} available via
     * {@link ISolution#getRule()}.
     */
    final int RULE = 1 << 2;

    /**
     * {@link #ELEMENT} and {@link #BINDINGS} and {@link #RULE}.
     */
    final int ALL = ELEMENT|BINDINGS|RULE;
    
    /**
     * Factory for {@link IBindingSet} implementations.
     * 
     * @param rule
     *            The rule whose bindings will be stored in the binding set.
     * 
     * @return A new binding set suitable for that rule.
     */
    public IBindingSet newBindingSet(IRule rule);
   
    /**
     * Return the effective {@link IRuleTaskFactory} for the rule. When the rule
     * is a step of a sequential program, then the returned {@link IStepTask}
     * must automatically flush the buffer after the rule executes.
     * 
     * @param parallel
     *            <code>true</code> unless the rule is a step is a sequential
     *            {@link IProgram}. Note that a sequential step MUST flush its
     *            buffer since steps are run in sequence precisely because they
     *            have a dependency!
     * @param rule
     *            A rule that is a step in some program. If the program is just
     *            a rule then the value of <i>parallel</i> does not matter. The
     *            buffer will is cleared when it flushed so a re-flushed is
     *            always a NOP.
     * 
     * @return The {@link IStepTask} to execute for that rule.
     * 
     * @see RunRuleAndFlushBufferTaskFactory
     * @see RunRuleAndFlushBufferTask
     */
    public IRuleTaskFactory getRuleTaskFactory(boolean parallel, IRule rule);
    
    /**
     * The timestamp used when an {@link IBuffer} is flushed against an
     * {@link IMutableRelation}.
     * <p>
     * Note: The {@link BTree} does NOT support concurrent writers. However,
     * {@link IStep}s are often executed in parallel. Therefore it is important
     * to use a {@link #getReadTimestamp()} that is different from the
     * {@link #getWriteTimestamp()}. Failure to do this will result in various
     * exceptions thrown out of the {@link BTree} class.
     */
    long getWriteTimestamp();

    /**
     * The timestamp used when obtaining an {@link IAccessPath} to read on a
     * {@link IRelation}. When computing the closure of a set of {@link IRule}s,
     * it is beneficial to specify a read timestamp that remains fixed during
     * each round of closure. The timestamp that should be choosen is the last
     * commit time for the database prior to the execution of the round.
     * 
     * @param relationName
     *            The relation on which you will read.
     */
    long getReadTimestamp(String relationName);
    
    /**
     * Locate and return the view of the relation identified by the
     * {@link IPredicate}. The implementation must choose a view that will
     * accept writes iff this is a mutation operation and which is associated
     * with an appropriate timestamp.
     * 
     * @param pred
     *            The {@link IPredicate}, which MUST be the head of some
     *            {@link IRule}.
     * 
     * @return The {@link IRelation}, which will never be a fused view and
     *         which will accept writes iff the rules are being executed as a
     *         mutation operation.
     */
    IRelation getHeadRelationView(IPredicate pred);
    
    /**
     * Locate and return the view of the relation(s) identified by the
     * {@link IPredicate}.
     * <p>
     * Note: This method is responsible for returning a fused view when more
     * than one relation name was specified for the {@link IPredicate}. It
     * SHOULD be used whenever the {@link IRelation} is selected based on a
     * predicate in the tail of an {@link IRule} and could therefore be a fused
     * view of more than one relation instance. (The head of the {@link IRule}
     * must be a simple {@link IRelation} and not a view.)
     * <p>
     * Note: The implementation should choose the read timestamp for each
     * relation in the view using {@link #getReadTimestamp(String)}.
     * 
     * @param pred
     *            The {@link IPredicate}, which MUST be a tail from some {@link IRule}.
     * 
     * @return The {@link IRelation}, which might be a
     *         {@link RelationFusedView}.
     */
    IRelation getTailRelationView(IPredicate pred);

    /**
     * Used to locate indices, relations and relation containers.
     */
    IIndexManager getIndexManager();
 
    /**
     * Run as a query.
     * 
     * @param step
     *            The {@link IRule} or {@link IProgram}.
     * 
     * @return An iterator from which you can read the solutions.
     * 
     * @throws IllegalStateException
     *             unless this is an {@link ActionEnum#Query}.
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    IChunkedOrderedIterator<ISolution> runQuery(IStep step) throws Exception;

    /**
     * Run as mutation operation (it will write any solutions onto the relations
     * named in the head of the various {@link IRule}s).
     * 
     * @param step
     *            The {@link IRule} or {@link IProgram}.
     * 
     * @return The mutation count (#of distinct elements modified in the
     *         relation(s)).
     * 
     * @throws IllegalArgumentException
     *             unless {@link ActionEnum#isMutation()} is <code>true</code>.
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    long runMutation(IStep step) throws Exception;
    
    /**
     * Return a thread-safe buffer onto which the computed {@link ISolution}s
     * will be written. The client will drain {@link ISolution}s from buffer
     * using {@link IBlockingBuffer#iterator()}.
     */
    IBlockingBuffer<ISolution> newQueryBuffer();

    /**
     * Return a thread-safe buffer onto which the computed {@link ISolution}s
     * will be written. When the buffer is {@link IBuffer#flush() flushed} the
     * {@link ISolution}s will be inserted into the {@link IMutableRelation}.
     * 
     * @param relation
     *            The relation.
     * 
     * @return The buffer.
     */
    IBuffer<ISolution> newInsertBuffer(IMutableRelation relation);

    /**
     * Return a thread-safe buffer onto which the computed {@link ISolution}s
     * will be written. When the buffer is {@link IBuffer#flush() flushed} the
     * {@link ISolution}s will be deleted from the {@link IMutableRelation}.
     * 
     * @param relation
     *            The relation.
     * 
     * @return The buffer.
     */
    IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation);

    /**
     * Make the write sets visible, eg, by committing the store(s) having
     * buffered write sets.
     */
    void makeWriteSetsVisible();

}
