/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.bset;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.rdf.rules.TMUtility;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.util.concurrent.Haltable;

/**
 * The union of two or more {@link BindingSetPipelineOp} operators.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo I have some basic questions about the ability to use a UNION of two
 *       predicates in scale-out. I think that this might be more accurately
 *       modeled as the UNION of two joins. That is, rather than:
 * 
 *       <pre>
 *       JOIN( ...,
 *             UNION( foo.spo(A,loves,B),
 *                    bar.spo(A,loves,B) )
 *             )
 * </pre>
 * 
 *       using
 * 
 *       <pre>
 *       UNION( JOIN( ..., foo.spo(A,loves,B) ),
 *              JOIN( ..., bar.spo(A,loves,B) )
 *              )
 * </pre>
 * 
 *       which would be a binding set union rather than an element union.
 * 
 * @todo The union of access paths was historically handled by
 *       {@link RelationFusedView}. That class should be removed once queries
 *       are rewritten to use the union of joins.
 * 
 * @todo The {@link TMUtility} will have to be updated to use this operator
 *       rather than specifying multiple source "names" for the relation of the
 *       predicate.
 * 
 * @todo The FastClosureRuleTask will also need to be updated to use a
 *       {@link Union} over the joins rather than a {@link RelationFusedView}.
 */
public class Union extends BindingSetPipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param args
     *            Two or more operators whose union is desired.
     * @param annotations
     */
    public Union(final BindingSetPipelineOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        if (args.length < 2)
            throw new IllegalArgumentException();

    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new UnionTask(this, context));
        
    }

    /**
     * Pipeline union impl.
     * 
     * FIXME All this does is copy its inputs to its outputs. Since we only run
     * one chunk of input at a time, it seems that the easiest way to implement
     * a union is to have the operators in the union just target the same sink.
     */
    private static class UnionTask extends Haltable<Void> implements Callable<Void> {

        public UnionTask(//
                final Union op,//
                final BOpContext<IBindingSet> context
                ) {

            if (op == null)
                throw new IllegalArgumentException();
            if (context == null)
                throw new IllegalArgumentException();
        }
        
        public Void call() throws Exception {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException();
        }

    }
    
}
