/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.EdgeOnlyFilter;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.graph.impl.util.VertexDistribution;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Striterator;

public class SAILGASEngine extends GASEngine {

    public SAILGASEngine(int nthreads) {
        super(nthreads);
    }

    /**
     * Returns <code>false</code>. The openrdf {@link Value} interface does not
     * implement {@link Comparable} and can not be sorted without an external
     * {@link Comparator}. Also, the order over the external RDF {@link Value}
     * representations is unlikely to benefit any {@link Sail} that uses
     * disk-based indices since the on-disk indices typically use a different
     * order, e.g., over <code>int</code> identifiers.
     */
    @Override
    public boolean getSortFrontier() {
        return false;
    }

    static public class SAILGraphAccessor implements IGraphAccessor {

        private final SailConnection cxn;
        private final boolean includeInferred;
        private final Resource[] defaultContext;
        
        public SAILGraphAccessor(final SailConnection cxn) {
        
            this(cxn, true/* includeInferred */, (Resource) null/* defaultContext */);

        }

        /**
         * 
         * @param cxn
         *            The connection.
         * @param includeInferred
         *            When <code>true</code>, inferred triples will be visited
         *            as well as explicit triples.
         * @param defaultContext
         *            The default context(s) for the operation (optional).
         */
        public SAILGraphAccessor(final SailConnection cxn,
                final boolean includeInferred, final Resource... defaultContext) {
            if (cxn == null)
                throw new IllegalArgumentException();
            this.cxn = cxn;
            this.includeInferred = includeInferred;
            this.defaultContext = defaultContext;
        }

        @Override
        public void advanceView() {
            // NOP
        }

        @Override
        public long getEdgeCount(final IGASContext<?, ?, ?> ctx, final Value u,
                final EdgesEnum edges) {

            long n = 0L;
            
            final Iterator<Statement> itr = getEdges(ctx, u, edges);

            while (itr.hasNext()) {

                itr.next();
                
                n++;

            }

            return n;

        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<Statement> getEdges(final IGASContext<?, ?, ?> p,
                final Value u, final EdgesEnum edges) {

            try {
                switch (edges) {
                case NoEdges:
                    return EmptyIterator.DEFAULT;
                case InEdges:
                    return getEdges(true/* inEdges */, p, u);
                case OutEdges:
                    return getEdges(false/* inEdges */, p, u);
                case AllEdges: {
                    final IStriterator a = getEdges(true/* inEdges */, p, u);
                    final IStriterator b = getEdges(false/* outEdges */, p, u);
                    a.append(b);
                    return a;
                }
                default:
                    throw new UnsupportedOperationException(edges.name());
                }
            } catch (SailException ex) {
                throw new RuntimeException(ex);
            }

        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private IStriterator getEdges(final boolean inEdges,
                final IGASContext<?, ?, ?> ctx, final Value u)
                throws SailException {

            final URI linkTypeIV = (URI) ctx.getLinkType();
            if(linkTypeIV != null) {
                /*
                 * FIXME RDR: We need to use a union of access paths for link
                 * attributes for the generic SAIL since it does not have the
                 * concept of statements about statements. This will require
                 * applying the access paths that will visit the appropriate
                 * reified triples. This needs to be done for both the standard
                 * path and the POS optimization code path.
                 */
                throw new UnsupportedOperationException();
            }
            /*
             * Optimize case where P is a constant and O is known (2 bound).
             * 
             * P is a constant.
             * 
             * [u] gets bound on O.
             * 
             * We use the POS(C) index. The S values give us the out-edges for
             * that [u] and the specified link type.
             * 
             * FIXME POS OPTIMIZATION: write unit test for this option to make
             * sure that the right filter is imposed. write performance test to
             * verify expected benefit. Watch out for the in-edges vs out-edges
             * since only one is optimized.
             */
            final boolean posOptimization = linkTypeIV != null
                    && inEdges;

            final CloseableIteration<? extends Statement, SailException> citr;
            if (posOptimization) {
            
                /*
                 * POS(C)
                 * 
                 * Bind P as a constant.
                 * 
                 * Bind O for this key-range scan.
                 */
                citr = cxn.getStatements(null/* s */, (URI) linkTypeIV/* p */,
                        u/* o */, includeInferred, defaultContext);

            } else {
                
                /*
                 * SPO(C) or OSP(C)
                 */
                final Resource s;
                final Value o;
                
                if (inEdges) {
                    // in-edges: OSP / OCSP : [u] is the Object.
                    o = u;
                    s = null;
                } else {
                    // out-edges: SPO / (SPOC|SOPC) : [u] is the Subject.
                    s = (Resource) u;
                    o = null;
                }

                citr = cxn.getStatements(s, null /* p */, o, includeInferred,
                        defaultContext);

            }
            
            final IStriterator sitr = new Striterator(
                    new Sesame2BigdataIterator<Statement, SailException>(citr));

//            if (linkTypeIV != null && !posOptimization) {
//                /*
//                 * A link type constraint was specified, but we were not able to
//                 * use the POS(C) index optimization. In this case we have to
//                 * add a filter to impose that link type constraint.
//                 */
//                sitr.addFilter(new Filter() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public boolean isValid(final Object e) {
//                        return ((ISPO) e).p().equals(linkTypeIV);
//                    }
//                });
//            }
            
            /*
             * Optionally wrap the program specified filter. This filter will be
             * pushed down onto the index. If the index is remote, then this is
             * much more efficient. (If the index is local, then simply stacking
             * striterators is just as efficient.)
             */

//            return ctx.getConstrainEdgeFilter(sitr);
            sitr.addFilter(new EdgeOnlyFilter(ctx));

            return sitr;
            
        }

        @Override
        public VertexDistribution getDistribution(final Random r) {

            try {

                final VertexDistribution sample = new VertexDistribution(r);

                final CloseableIteration<? extends Statement, SailException> citr = cxn
                        .getStatements(null/* s */, null/* p */, null /* o */,
                                includeInferred, defaultContext);

                try {

                    while (citr.hasNext()) {

                        final Statement st = citr.next();

                        if (!(st.getObject() instanceof Resource)) {
                            
                            // This is a property value, not an edge.
                            continue;
                        }

                        if (st.getSubject().equals(st.getObject())) {

                            // ignore self-loops.
                            continue;

                        }

                        sample.addOutEdgeSample(st.getSubject());

                        sample.addInEdgeSample((Resource) st.getObject());

                    }

                } finally {

                    citr.close();

                }

                return sample;

            } catch (SailException ex) {

                throw new RuntimeException(ex);
                
            }
            
        }

    }

}
