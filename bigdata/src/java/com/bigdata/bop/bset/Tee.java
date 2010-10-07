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
 * Created on Sep 20, 2010
 */

package com.bigdata.bop.bset;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.Union;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.rdf.rules.TMUtility;
import com.bigdata.relation.RelationFusedView;
import com.bigdata.relation.rule.Slice;

/**
 * TEE(op):[sinkRef=X; altSinkRef=Y]
 * <p>
 * Pipeline operator copies its source to both sink and altSink. The sink and
 * the altSink must both be ancestors of the operator. The sinkRef MAY be
 * omitted when one of the targets is the immediate parent of the TEE.
 * Evaluation scope: {@link BOpEvaluationContext#ANY}.
 * <p>
 * <h2>Example - Truth Maintenance</h2>
 * <p>
 * In truth maintenance we establish a focus store which is brought to a fixed
 * point by applying some rules and a transitive closure operator. Once the
 * fixed point is reached, the assertions in the focus store are either inserted
 * onto the database or (for retraction) removed from database unless a proof
 * can be found that an assertion is still entailed.
 * <p>
 * The {@link Tee} operator can be used in truth maintenance to read on the
 * UNION of the focus store and the database - see {@link TMUtility}. This is
 * handled as the "union" of two JOINs using a {@link Tee} as follows:
 * 
 * <pre>
 *       slice := SLICE( join2 )[bopId=3]
 *       join2 := JOIN( join1, bar.spo(A,loves,B))[bopId=2]
 *       join1 := JOIN(   tee, foo.spo(A,loves,B))[bopId=1; sinkRef=3]
 *       tee   := TEE( ... )[altSinkRef=2],
 * </pre>
 * 
 * The {@link Tee} copies its inputs to both the default sink (its parent, which
 * is join1) and the alternate sink (join2). join1 routes its outputs around
 * join2, sending them directly to their lowest common ancestor. This has the
 * effect of creating a union of their outputs at the receiver. In this example,
 * a {@link Slice} is used as the target for both of the join operators. Since
 * this is a pipeline construction, the joins will be evaluated in parallel as
 * intermediate results arrive for those operators. Normally the {@link Tee}
 * will be fed by a {@link StartOp} or another {@link PipelineJoin}.
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
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Tee extends CopyOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Deep copy constructor.
     * @param op
     */
    public Tee(final Tee op) {
        super(op);
    }

    /**
     * Shallow copy constructor.
     * @param args
     * @param annotations
     */
    public Tee(BOp[] args, Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(PipelineOp.Annotations.ALT_SINK_REF);

    }
    
}
