/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.bop.joinGraph.rto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.ap.SampleIndex;
import com.bigdata.bop.ap.SampleIndex.SampleType;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedIterator;

/**
 * A vertex of the join graph is an annotated relation (this corresponds to an
 * {@link IPredicate} with additional annotations to support the adaptive query
 * optimization algorithm).
 * <p>
 * The unique identifier for a {@link Vertex} (within a given join graph) is the
 * {@link BOp.Annotations#BOP_ID} decorating its {@link IPredicate}.
 * {@link #hashCode()} is defined in terms of this unique identifier so we can
 * readily detect when a {@link Set} already contains a given {@link Vertex}.
 */
public class Vertex implements Serializable {

    private static final transient Logger log = Logger.getLogger(Vertex.class);

    private static final long serialVersionUID = 1L;

    /**
     * The {@link IPredicate} associated with the {@link Vertex}. This basically
     * provides the information necessary to select an appropriate access path.
     */
    public final IPredicate<?> pred;

    /**
     * The most recently taken sample of the {@link Vertex}.
     */
    transient VertexSample sample = null;

    Vertex(final IPredicate<?> pred) {

        if (pred == null)
            throw new IllegalArgumentException();

        this.pred = pred;

    }

    public String toString() {

        return "Vertex{pred=" + pred + ",sample=" + sample + "}";

    }

    /**
     * Equals is based on a reference test.
     */
    public boolean equals(Object o) {
        return this == o;
    }

    /**
     * The hash code is just the {@link BOp.Annotations#BOP_ID} of the
     * associated {@link IPredicate}.
     */
    public int hashCode() {
        return pred.getId();
    }

    /**
     * Take a sample of the vertex, updating {@link #sample} as a side-effect.
     * If the sample is already exact, then this is a NOP. If the vertex was
     * already sampled to that limit, then this is a NOP (you have to raise the
     * limit to re-sample the vertex).
     * 
     * @param limit
     *            The sample cutoff.
     */
    @SuppressWarnings("unchecked")
    public void sample(final QueryEngine queryEngine, final int limit,
            final SampleType sampleType) {

        if (queryEngine == null)
            throw new IllegalArgumentException();

        if (limit <= 0)
            throw new IllegalArgumentException();

        if (sampleType == null)
            throw new IllegalArgumentException();

        final VertexSample oldSample = this.sample;

        if (oldSample != null && oldSample.estimateEnum == EstimateEnum.Exact) {

            /*
             * The old sample is already the full materialization of the vertex.
             */

            return;

        }

        if (oldSample != null && oldSample.limit >= limit) {

            /*
             * The vertex was already sampled to this limit.
             */

            return;

        }

        final BOpContextBase context = new BOpContextBase(queryEngine);

        final IRelation r = context.getRelation(pred);

        final IAccessPath ap = context.getAccessPath(r, pred);

        final long rangeCount = oldSample == null ? ap
                .rangeCount(false/* exact */) : oldSample.estCard;

        if (rangeCount <= limit) {

            /*
             * Materialize the access path.
             * 
             * TODO This could be more efficient if we raised it onto the AP or
             * if we overrode CHUNK_CAPACITY and the fully buffered iterator
             * threshold such that everything was materialized as a single
             * chunk.
             */

            final List<Object> tmp = new ArrayList<Object>((int) rangeCount);

            final IChunkedIterator<Object> itr = ap.iterator();

            try {

                while (itr.hasNext()) {

                    tmp.add(itr.next());

                }

            } finally {

                itr.close();
            }

            sample = new VertexSample(rangeCount, limit, EstimateEnum.Exact,
                    elementsToBindingSets(pred, tmp.toArray(new Object[tmp
                            .size()])));

        } else {

            /*
             * Materialize a sample from the access path.
             */

            final SampleIndex<?> sampleOp = new SampleIndex(new BOp[] {}, //
                    NV.asMap(//
                            new NV(SampleIndex.Annotations.PREDICATE, pred),//
                            new NV(SampleIndex.Annotations.LIMIT, limit),//
                            new NV(SampleIndex.Annotations.SAMPLE_TYPE,
                                    sampleType.name())//
                            ));

            sample = new VertexSample(rangeCount, limit, EstimateEnum.Normal,
                    elementsToBindingSets(pred, sampleOp.eval(context)));

        }

        if (log.isTraceEnabled())
            log.trace("Sampled: id=" + pred.getId() + ", sample=" + sample);

        return;

    }

    /**
     * Convert the source sample into an IBindingSet[].
     * 
     * @param pred
     *            The {@link IPredicate}, which tells us the variables which
     *            need to become bound.
     * @param elements
     *            The sampled elements as materialized from the index.
     */
    static private IBindingSet[] elementsToBindingSets(
            final IPredicate<?> pred, final Object[] elements) {

        final IBindingSet[] sourceSample = new IBindingSet[elements.length];

        for (int i = 0; i < sourceSample.length; i++) {

            final IBindingSet bset = new HashBindingSet();

            BOpContext.copyValues((IElement) elements[i], pred, bset);

            sourceSample[i] = bset;
        }

        return sourceSample;

    }
    
}
