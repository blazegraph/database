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
 * Created on Nov 7, 2011
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.HTreeNamedSubqueryOp;
import com.bigdata.bop.controller.NamedSolutionSetRef;
import com.bigdata.htree.HTree;

/**
 * An N-way merge join, for N GTE 2.
 * 
 * <h2>Algorithm</h2>
 * 
 * The merge join takes a set of solution sets in the same order and some join
 * variables. It examines the next solution for each set and compares them,
 * finding the solution which has the earliest order among those solutions on
 * the join variables. For each solution set which reported a solution having
 * the same join variables as that earliest solution, it outputs the cross
 * product and advances the iterator on that solution set.
 * 
 * <p>
 * 
 * For the {@link HTree}, the entries are in key order. Those keys are hash
 * codes computed from the solutions using the join variables. While this does
 * not change the fact that the {@link HTree} puts an order over the solutions,
 * there is a wrinkle as the hash code represents a collision bucket - a set of
 * solutions which have the same hash code. Thus we need to order the solutions
 * in each collision bucket. To do this, the iterator must advance until the key
 * changes, which signifies a new hash code and hence a new collision bucket.
 * The solutions in the last collision bucket must be placed into a total order
 * and delivered to the merge join algorithm in that total order.
 * <p>
 * If the MERGE JOIN is REQUIRED, then we want to synchronize the source
 * {@link HTree} iterators on the next lowest key (aka hash code) which they all
 * have in common.
 * <p>
 * If the MERGE JOIN is OPTIONAL, then we want to synchronize the source
 * {@link HTree} iterators on the next lowest key (aka hash code) which appears
 * for any source iterator. Solutions will not be drawn from iterators not
 * having that key in that pass.
 * 
 * TODO We could do the same thing with JVM hash indices. The
 * {@link JVMHashJoinUtility} explicitly models those collision buckets.
 * 
 * TODO Need to specify a {@link NamedSolutionSetRef}[] for this operator. This
 * makes it very {@link HTree} specific.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HTreeMergeJoin extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AccessPathJoinAnnotations,
            HTreeHashJoinAnnotations {

        /**
         * The {@link NamedSolutionSetRef}[] used to locate the named solution
         * sets.  There must be at least 2 entries in the array.
         * 
         * @see NamedSolutionSetRef
         * @see HTreeNamedSubqueryOp.Annotations#NAMED_SET_REF
         */
        final String NAMED_SET_REF = HTreeNamedSubqueryOp.Annotations.NAMED_SET_REF;
        
        /**
         * Constraints to be applied by the join (in addition to any associated
         * with the {@link HTreeHashJoinUtility} state in the
         * {@link #NAMED_SET_REF}).
         */
        final String CONSTRAINTS = JoinAnnotations.CONSTRAINTS;
        
//        /**
//         * When <code>true</code> the hash index identified by
//         * {@link #NAMED_SET_REF} will be released when this operator is done
//         * (default {@value #DEFAULT_RELEASE}).
//         * <p>
//         * Note: Whether or not the hash index can be released depends on
//         * whether or not the hash index will be consumed by more than one
//         * operator in the query plan. For example, a named solution set can be
//         * consumed by more than one operator and thus must not be released
//         * until all such operators are done.
//         * 
//         * TODO Alternatively, we could specify the #of different locations in
//         * the query plan where the named solution set will be consumed. This
//         * could be part of the {@link HTreeHashJoinUtility} state, in which
//         * case it would only be set as an annotation on the operator which
//         * generates the hash index.
//         */
//        final String RELEASE = HTreeSolutionSetHashJoinOp.class + ".release";
//
//        final boolean DEFAULT_RELEASE = true;
        
    }

    /**
     * @param args
     * @param annotations
     */
    public HTreeMergeJoin(BOp[] args, Map<String, Object> annotations) {
        super(args, annotations);
    }

    /**
     * @param op
     */
    public HTreeMergeJoin(HTreeMergeJoin op) {
        super(op);
    }

    public HTreeMergeJoin(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }
    
    @Override
    public FutureTask<Void> eval(BOpContext<IBindingSet> context) {
        // TODO Auto-generated method stub
        return null;
    }

}
