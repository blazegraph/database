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

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.rdf.rules.TMUtility;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.util.concurrent.Haltable;

/**
 * UNION(ops)[maxParallel(default all)]
 * <p>
 * Executes each of the operands in the union as a subqueries. Each subquery is
 * run as a separate query but is linked to the parent query in which the UNION
 * is being evaluated. The subqueries do not receive bindings from the parent
 * and may be executed independently. By default, the subqueries are run with
 * unlimited parallelism.
 * <p>
 * UNION is useful when independent queries are evaluated and their outputs are
 * merged. Outputs from the UNION operator flow to the parent operator and will
 * be mapped across shards or nodes as appropriate for the parent. UNION runs on
 * the query controller. In order to avoid routing intermediate results through
 * the controller, the {@link BindingSetPipelineOp.Annotations#SINK_REF} of each
 * child operand should be overridden to specify the parent of the UNION
 * operator.
 * <p>
 * UNION can not be used when the intermediate results must be routed into the
 * subqueries.  However, a {@link Tee} pattern may help in such cases.  For
 * example, a {@link Tee} may be used to create a union of pipeline joins for
 * two access paths during truth maintenance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
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

//        return new FutureTask<Void>(new UnionTask(this, context));
        throw new UnsupportedOperationException();
    }

//    /**
//     * Pipeline union impl.
//     * 
//     * FIXME All this does is copy its inputs to its outputs. Since we only run
//     * one chunk of input at a time, it seems that the easiest way to implement
//     * a union is to have the operators in the union just target the same sink.
//     */
//    private static class UnionTask extends Haltable<Void> implements Callable<Void> {
//
//        public UnionTask(//
//                final Union op,//
//                final BOpContext<IBindingSet> context
//                ) {
//
//            if (op == null)
//                throw new IllegalArgumentException();
//            if (context == null)
//                throw new IllegalArgumentException();
//        }
//        
//        public Void call() throws Exception {
//            // TODO Auto-generated method stub
//            throw new UnsupportedOperationException();
//        }
//
//    }
    
}
