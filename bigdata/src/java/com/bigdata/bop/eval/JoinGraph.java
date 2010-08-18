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
 * Created on Aug 16, 2010
 */

package com.bigdata.bop.eval;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.AbstractPipelineOp;
import com.bigdata.bop.BOp;
import com.bigdata.bop.BindingSetPipelineOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.service.IBigdataFederation;

/**
 * A join graph with annotations for estimated cardinality and other details in
 * support of runtime query optimization. A join graph is a collection of
 * relations and joins which connect those relations.
 * <p>
 * 
 * @see http://arxiv.org/PS_cache/arxiv/pdf/0810/0810.4809v1.pdf, XQuery Join
 *      Graph Isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Some edges can be eliminated by transitivity. For example, given
 * 
 *       <pre>
 * query:
 * 
 * :- (A loves B), (B loves A), (B marriedTo C).
 * 
 * vertices: 
 * 
 * v1=(A loves B);
 * v2=(B loves A);
 * v3=(B marriedTo C);
 * 
 * edges:
 * 
 * e1=(v1,v2) // JOIN( SCAN(A loves B), SCAN(B loves A)) 
 * e2=(v1,v3) // JOIN( SCAN(A loves B), SCAN(B marriedTo C))
 * e3=(v2,v3) // JOIN( SCAN(B loves A), SCAN(B marriedTo C))
 *   
 * It is necessary to execute e1 and either e2 or e3, but not both e2 and e3.
 * </pre>
 * 
 * @todo In order to combine pipelining with runtime query optimization we need
 *       to sample based on the first chunk(s) delivered by the pipeline. If
 *       necessary, we can buffer multiple chunks for semi-selective queries.
 *       However, for unselective queries we would accept as many buffers worth
 *       of bindings as were allowed for a given join and then sample the
 *       binding sets from left hand side (the buffers) and run those samples
 *       against the right hand side (the local shard).
 */
public class JoinGraph extends AbstractPipelineOp<IBindingSet> implements
        BindingSetPipelineOp {

    private static final long serialVersionUID = 1L;

    /**
     * Known annotations.
     */
    public interface Annotations extends BOp.Annotations {
        /**
         * The default sample size (100 is a good value).
         */
        String SAMPLE_SIZE = "sampleSize";
    }

    /**
     * Vertices of the join graph.
     */
    private final Vertex[] V;

    /**
     * Edges of the join graph.
     */
    private final Edge[] E;

    /**
     * A vertex of the join graph is an annotated relation (this corresponds to
     * an {@link IPredicate} with additional annotations to support the adaptive
     * query optimization algorithm).
     */
    private static class Vertex implements Serializable {
        
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        final IPredicate<?> pred;

        Vertex(final IPredicate<?> pred) {
            if (pred == null)
                throw new IllegalArgumentException();
            this.pred = pred;
        }
    }

    /**
     * An edge of the join graph is an annotated join operator. The edges of the
     * join graph are undirected. Edges exist when the vertices share at least
     * one variable.
     */
    private static class Edge implements Serializable {
        
        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * The vertices connected by that edge.
         */
        final Vertex v1, v2;

        /**
         * A weight representing the estimated cardinality of the join.
         */
        double w;

        public Edge(final Vertex v1, final Vertex v2) {
            if (v1 == null)
                throw new IllegalArgumentException();
            if (v2 == null)
                throw new IllegalArgumentException();
            this.v1 = v1;
            this.v2 = v2;
        }
    }

    /**
     * 
     * @param joinNexus
     * @param v
     * @param sampleSize
     *            The default sample size to use when sampling a vertex of the
     *            join graph (100).
     * 
     * @todo We can derive the vertices from the join operators or the join
     *       operators from the vertices. However, if a specific kind of join
     *       operator is required then the question is whether we have better
     *       information to make that choice when the join graph is evaluated or
     *       before it is constructed.
     */
    public JoinGraph(final IPredicate<?>[] v, final int sampleSize) {

        super(v/* args */, NV.asMap(new NV[] {//
                new NV(Annotations.SAMPLE_SIZE, Integer.valueOf(sampleSize))//
                }));

        if (v == null)
            throw new IllegalArgumentException();

        if (sampleSize <= 0)
            throw new IllegalArgumentException();

        V = new Vertex[v.length];

        for (int i = 0; i < v.length; i++) {

            V[i] = new Vertex(v[i]);
            
        }

        /*
         * Identify the edges by looking for shared variables among the
         * predicates.
         */
        {

            final List<Edge> tmp = new LinkedList<Edge>();

            for (int i = 0; i < v.length; i++) {

                final IPredicate<?> p1 = v[i];

                for (int j = i + 1; j < v.length; j++) {

                    final IPredicate<?> p2 = v[j];

                    final Set<IVariable<?>> shared = Rule.getSharedVars(p1, p2);

                    if (shared != null) {

                        tmp.add(new Edge(V[i], V[j]));

                    }

                }

            }
         
            E = tmp.toArray(new Edge[0]);
            
        }

    }

    public Future<Void> eval(final IBigdataFederation<?> fed,
            final IJoinNexus joinNexus,
            final IBlockingBuffer<IBindingSet[]> buffer) {

        final FutureTask<Void> ft = new FutureTask<Void>(new JoinGraphTask(
                joinNexus, buffer));

        buffer.setFuture(ft);
        
        joinNexus.getIndexManager().getExecutorService().execute(ft);

        return ft;
        
    }

    /**
     * Evaluation of a {@link JoinGraph}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private class JoinGraphTask implements Callable<Void> {

        private final IJoinNexus joinNexus;

        private final IBlockingBuffer<IBindingSet[]> buffer;

        JoinGraphTask(final IJoinNexus joinNexus,
                final IBlockingBuffer<IBindingSet[]> buffer) {

            if (joinNexus == null)
                throw new IllegalArgumentException();

            if (buffer == null)
                throw new IllegalArgumentException();

            this.joinNexus = joinNexus;

            this.buffer = buffer;

        }

        public Void call() throws Exception {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException();
        }

        public IRelation<?> getRelation(final String v) {

            return (IRelation<?>) joinNexus.getIndexManager()
                    .getResourceLocator().locate(v/* namespace */,
                            joinNexus.getReadTimestamp());

            // return joinNexus.getTailRelationView(pred)

        }

    }

}
