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
 * Created on Sep 3, 2008
 */

package com.bigdata.relation.rule;

import java.io.Serializable;

import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.eval.IEvaluationPlan;
import com.bigdata.relation.rule.eval.IJoinNexus;

/**
 * An interface for expander patterns for an {@link IPredicate} when it appears
 * in the right-hand position of a JOIN.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISolutionExpander<E> extends Serializable {

//    /**
//     * This method is invoked for each solution for a JOIN. The implementation
//     * may cause zero or more solutions to be emitted each time it is invoked.
//     * If the implementation does nothing, then the solution will be discarded.
//     * If the implementation simply emits the solution represented by the
//     * {@link IBindingSet} then the implementation has the semantics of a NOP.
//     * <p>
//     * In addition, if {@link IPredicate#isOptional()} then the
//     * {@link ISolutionExpander} is invoked one more time once all solutions in
//     * the data have been exhausted (and exactly once if there are no solutions
//     * in the data).
//     * 
//     * @param joinNexus
//     * @param bindingSet
//     * @param predicate
//     * @param isSolution
//     */
//    void expand(IJoinNexus joinNexus, IBindingSet bindingSet,
//            IPredicate<E> predicate, boolean isSolution);
//    
//    /**
//     * Return the range count for the {@link IPredicate}. This may be used to
//     * override the default rangeCount behavior. The returned value MAY be
//     * approximate and is ONLY used when computing an {@link IEvaluationPlan}.
//     * In general, the return value will be cached so this method will not be
//     * invoked more than once for a given {@link IPredicate} during the
//     * evaluation of an {@link IRule}.
//     * 
//     * @param accessPath
//     *            The {@link IAccesspath} obtained for the {@link IPredicate}.
//     *            This MAY be based on a fused view of {@link IRelation}s for
//     *            some evaluation context. For example, truth maintenance for
//     *            the RDF DB uses a fused view.
//     * 
//     * @return The #of elements that will be visited by an {@link IAccessPath}
//     *         reading on the {@link IPredicate} assuming NO bindings.
//     */
//    long rangeCount(IAccessPath<E> accessPath);
    
    /**
     * Return the {@link IAccessPath} that will be used to evaluate the
     * {@link IPredicate}.
     * 
     * @param accessPath
     *            The {@link IAccessPath} that will be used by default.
     * 
     * @return The {@link IAccessPath} that will be used. You can return the
     *         given <i>accessPath</i> or you can layer additional semantics
     *         onto or otherwise override the given {@link IAccessPath}.
     * 
     * @todo just fold this into {@link IPredicate} in place of
     *       {@link IPredicate#getSolutionExpander()} and drop that method.
     */
    IAccessPath<E> getAccessPath(IAccessPath<E> accessPath);
    
    /**
     * Add the backchainer on top of the expander.
     * 
     * @return true if the backchainer should run
     */
    boolean backchain();
    
    /**
     * If true, the predicate for this expander will be given priority in the
     * join order.
     * 
     *  @return true if the predicate should be run first
     */
    boolean runFirst();
    
}
