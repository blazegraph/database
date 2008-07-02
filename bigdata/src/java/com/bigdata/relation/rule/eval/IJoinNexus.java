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
import java.util.concurrent.ExecutorService;

import com.bigdata.btree.BTree;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
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
     * Create a new element. The element is constructed from the bindings for
     * the head of a rule.
     * 
     * @param predicate
     *            The predicate that is the head of some {@link IRule}.
     * @param bindingSet
     *            A set of bindings for that {@link IRule}.
     * 
     * @return The new element.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is <code>null</code>.
     * @throws IllegalStateException
     *             if the predicate is not fully bound given those bindings.
     */
    Object newElement(IPredicate predicate, IBindingSet bindingSet);

    /**
     * Create a new {@link ISolution}.
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
     */
    ISolution newSolution(IRule rule, IBindingSet bindingSet);

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
     */
    long getReadTimestamp();
    
    /**
     * The service that should be used to run tasks such as parallel rule
     * execution.
     */
    ExecutorService getExecutorService();
 
    /**
     * The object responsible for resolving {@link IRelationName}s to
     * {@link IRelation}s.
     */
    IRelationLocator getRelationLocator();
    
    /**
     * Run as a query.
     * 
     * @param step
     *            The {@link IRule} or {@link IProgram}.
     * 
     * @return An iterator from which you can read the solutions.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    IChunkedOrderedIterator<ISolution> runQuery(IStep step) throws Exception;

    /**
     * Run as mutation operation (it will write any solutions onto the relations
     * named in the head of the various {@link IRule}s).
     * 
     * @param action
     *            The action.
     * @param step
     *            The {@link IRule} or {@link IProgram}.
     * 
     * @return The mutation count (#of distinct elements modified in the
     *         relation(s)).
     * 
     * @throws IllegalArgumentException
     *             unless <i>action</i> is a mutation (insert or delete).
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    long runMutation(ActionEnum action, IStep step) throws Exception;
    
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

}
