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
package com.bigdata.rdf.graph.impl.ram;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
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

public class RAMGASEngine extends GASEngine {

    public RAMGASEngine(int nthreads) {
        super(nthreads);
    }

    /**
     * Returns <code>false</code>. There is no intrinsic ordering that can
     * improve access for this implementation.
     */
    @Override
    public boolean getSortFrontier() {
        return false;
    }

    /**
     * A simple RDF graph model suitable for graph mining algorithms.
     * 
     * TODO This model does not support link weights. It was developed to
     * provide an implementation without any object encode/decode overhead that
     * could be used to explore the possible performance of GAS algorithms under
     * Java.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    static public class RAMGraph {
        
        private final ValueFactory vf;
        public ValueFactory getValueFactory() {
            return vf;
        }
        
        /**
         * From a vertex, we can visit the in-edges, out-edges, or attribute
         * values. These things are organized into three sets of statements. A
         * non-thread-safe collection is used to provide the distinct semantics
         * for those sets and fast traversal. This design precludes the ability
         * to concurrently modify the graph during graph traversal operations.
         */
        static private class Vertex {
            
            /** The {@link Value} for that {@link Vertex}. */
            final private Value v;
            /**
             * The distinct set of in-edges for this {@link Vertex}.
             * <p>
             * The {@link Statement#getObject()} for each {@link Statement} in
             * this collection will be the {@link #v}.
             */
            private Set<Statement> inEdges = null;
            /**
             * The distinct set of out-edges for this {@link Vertex}.
             * <p>
             * The {@link Statement#getSubject()} for each {@link Statement} in
             * this collection will be the {@link #v}.
             */
            private Set<Statement> outEdges = null;
            /**
             * The distinct set of property values for this {@link Vertex}.
             * <p>
             * The {@link Statement#getSubject()} for each {@link Statement} in
             * this collection will be the {@link #v}.
             * <p>
             * The {@link Statement#getObject()} for each {@link Statement} in
             * this collection will be a {@link URI}.
             */
            private Set<Statement> attribs = null;

            public Vertex(final Value v) {
                if (v == null)
                    throw new NullPointerException();
                this.v = v;
            }
            @Override
            public String toString() {
                return "Vertex{" + v + ",inEdges=" + getInEdgeCount()
                        + ",outEdges=" + getOutEdgeCount() + ",attribs="
                        + getAttribCount() + "}";
            }
            
            private boolean addAttrib(final Statement s) {
                if (attribs == null) {
                    attribs = new LinkedHashSet<Statement>();
                }
                return attribs.add(s);
            }

            private boolean addOutEdge(final Statement s) {
                if (outEdges == null) {
                    outEdges = new LinkedHashSet<Statement>();
                }
                return outEdges.add(s);
            }

            private boolean addInEdge(final Statement s) {
                if (inEdges == null) {
                    inEdges = new LinkedHashSet<Statement>();
                }
                return inEdges.add(s);
            }

            public int getAttribCount() {
                return attribs == null ? 0 : attribs.size();
            }

            public int getInEdgeCount() {
                return inEdges == null ? 0 : inEdges.size();
            }

            public int getOutEdgeCount() {
                return outEdges == null ? 0 : outEdges.size();
            }

            public Iterator<Statement> inEdges() {
                if (inEdges == null)
                    return EmptyIterator.DEFAULT;
                return inEdges.iterator();
            }

            public Iterator<Statement> outEdges() {
                if (outEdges == null)
                    return EmptyIterator.DEFAULT;
                return outEdges.iterator();
            }

            public Iterator<Statement> attribs() {
                if (attribs == null)
                    return EmptyIterator.DEFAULT;
                return attribs.iterator();
            }
            
        }
        
        /**
         * The vertices. 
         */
        private final ConcurrentMap<Value,Vertex> vertices;

        public RAMGraph() {

            vertices = new ConcurrentHashMap<Value, Vertex>();
            
            vf = new ValueFactoryImpl();
            
        }
        
        /**
         * Lookup / create a vertex.
         * 
         * @param x
         *            The {@link Value}.
         * @param create
         *            when <code>true</code> the {@link Vertex} will be created
         *            if it does not exist.
         * 
         * @return The {@link Vertex}.
         */
        private Vertex get(final Value x, final boolean create) {

            Vertex v = vertices.get(x);

            if (v == null && create) {

                final Vertex oldVal = vertices
                        .putIfAbsent(x, v = new Vertex(x));

                if (oldVal != null) {

                    // lost data race.
                    v = oldVal;

                }

            }

            return v;
            
        }
        
        public boolean add(final Statement st) {

            final Resource s = st.getSubject();
            
            final Value o = st.getObject();

            boolean modified = false;
            if (o instanceof URI) {
                // Edge
                modified |= get(s, true/* create */).addOutEdge(st);
                modified |= get(o, true/* create */).addInEdge(st);
            } else {
                // Property value.
                modified |= get(s, true/* create */).addAttrib(st);
            }
            return modified;

        }

        public Iterator<Statement> inEdges(final Value v) {
            final Vertex x = get(v, false/* create */);
            if (x == null)
                return EmptyIterator.DEFAULT;
            return x.inEdges();
        }
        
        public Iterator<Statement> outEdges(final Value v) {
            final Vertex x = get(v, false/* create */);
            if (x == null)
                return EmptyIterator.DEFAULT;
            return x.outEdges();
        }
        public Iterator<Statement> attribs(final Value v) {
            final Vertex x = get(v, false/* create */);
            if (x == null)
                return EmptyIterator.DEFAULT;
            return x.attribs();
        }
        
    } // class RAMGraph
    
    static public class RAMGraphAccessor implements IGraphAccessor {
        
        private final RAMGraph g;
        
        public RAMGraphAccessor(final RAMGraph g) {
            if (g == null)
                throw new IllegalArgumentException();
            this.g = g;
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
        public Iterator<Statement> getEdges(final IGASContext<?, ?, ?> ctx,
                final Value u, final EdgesEnum edges) {

            try {
                switch (edges) {
                case NoEdges:
                    return EmptyIterator.DEFAULT;
                case InEdges:
                    return getEdges(true/* inEdges */, ctx, u);
                case OutEdges:
                    return getEdges(false/* inEdges */, ctx, u);
                case AllEdges: {
                    final IStriterator a = getEdges(true/* inEdges */, ctx, u);
                    final IStriterator b = getEdges(false/* outEdges */, ctx, u);
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
            final Striterator sitr;
            if(inEdges) {
                sitr = new Striterator(g.get(u, false/*create*/).inEdges());
            } else {
                sitr = new Striterator(g.get(u, false/*create*/).outEdges());
            }
            /*
             * Optionally wrap the program specified filter. 
             */
//            return ctx.getConstrainEdgeFilter(sitr);
            sitr.addFilter(new EdgeOnlyFilter(ctx));
            
            return sitr;

        }

        @Override
        public VertexDistribution getDistribution(final Random r) {

            final VertexDistribution sample = new VertexDistribution(r);

            for (RAMGraph.Vertex vertex : g.vertices.values()) {

                final Value v = vertex.v;
                
                if (v instanceof Resource) {

                    /*
                     * FIXME This is not ignoring self-loops. Realistically, we
                     * want to include them in the data since they are part of
                     * the data, but we do not want to consider them in samples
                     * since they do not actually go anywhere. The SAIL and BD
                     * implementations of this method filter out self-loops, but
                     * this implementation does not.
                     */
                    
                    if (vertex.getInEdgeCount() > 0)
                        sample.addInEdgeSample((Resource) v);

                    if (vertex.getOutEdgeCount() > 0)
                        sample.addOutEdgeSample((Resource) v);

                }

            }

            return sample;

        }

    }

}
