package com.bigdata.rdf.graph.impl.bd;

import java.lang.ref.WeakReference;
import java.util.Iterator;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.graph.EdgesEnum;
import com.bigdata.rdf.graph.IGASContext;
import com.bigdata.rdf.graph.IGASEngine;
import com.bigdata.rdf.graph.IGASProgram;
import com.bigdata.rdf.graph.IGASSchedulerImpl;
import com.bigdata.rdf.graph.IGASState;
import com.bigdata.rdf.graph.IGraphAccessor;
import com.bigdata.rdf.graph.impl.GASContext;
import com.bigdata.rdf.graph.impl.GASEngine;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.striterator.Resolver;
import com.bigdata.striterator.Striterator;

/**
 * {@link IGASEngine} for dynamic activation of vertices. This implementation
 * maintains a frontier and lazily initializes the vertex state when the vertex
 * is visited for the first time. This is appropriate for algorithms, such as
 * BFS, that use a dynamic frontier.
 * 
 * <h2>Dynamic Graphs</h2>
 * 
 * There are at least two basic approaches to computing an analytic over a
 * dynamic graph.
 * <p>
 * The first way to compute an analytic over a dynamic graph is to specify the
 * timestamp of the view as {@link ITx#READ_COMMITTED}. The view of the graph
 * <em>in each round</em> will be automatically advanced to the most recently
 * committed view of that graph. Thus, if there are concurrent commits, each
 * time the {@link IGASProgram} is executed within a given round of evaluation,
 * it will see the most recently committed state of the data graph.
 * <p>
 * The second way to compute an analytic over a dynamic graph is to explicitly
 * change the view before each round. This can be achieved by tunneling the
 * {@link BigdataGraphAccessor} interface from
 * {@link IGASProgram#nextRound(IGASContext)}. If you take this approach, then
 * you could explicitly walk through an iterator over the commit record index
 * and update the timestamp of the view. This approach allows you to replay
 * historical committed states of the graph at a known one-to-one rate (one
 * graph state per round of the GAS computation).
 * 
 * TODO Algorithms that need to visit all vertices in each round (CC, BC, PR)
 * can be more optimially executed by a different implementation strategy. The
 * vertex state should be arranged in a dense map (maybe an array) and presized.
 * For example, this could be done on the first pass when we identify a vertex
 * index for each distinct V in visitation order.
 * 
 * TODO Vectored expansion with conditional materialization of attribute values
 * could be achieved using CONSTRUCT. This would force URI materialization as
 * well. If we drop down one level, then we can push in the frontier and avoid
 * the materialization. Or we can just write an operator that accepts a frontier
 * and returns the new frontier and which maintains an internal map containing
 * both the visited vertices, the vertex state, and the edge state.
 * 
 * TODO Some computations could be maintained and accelerated. A great example
 * is Shortest Path (as per RDF3X). Reachability queries for a hierarchy can
 * also be maintained and accelerated (again, RDF3X using a ferrari index).
 * 
 * TODO Option to materialize Literals (or to declare the set of literals of
 * interest) [Note: We can also require that people inline all URIs and Literals
 * if they need to have them materialized, but a materialization filter for
 * Gather and Scatter would be nice if it can be selective for just those
 * attributes or vertex identifiers that matter).
 * 
 * TODO DYNAMIC GRAPHS: Another possibility would be to replay a history log,
 * explicitly making changes to the graph. In order to provide high concurrency
 * for readers, this would require a shadowing of the graph (effectively,
 * shadowing the indices). That might be achieved by replaying the changes into
 * a version fork of the graph and then using a read-only view of the fork. This
 * is basically a retroactive variant of replaying the commit points from the
 * commit record index. I am not sure if it has much to recommend it.
 * <p>
 * The thing that is interesting about the history index, is that it captures
 * just the delta. Actually computing the delta between two commit points is
 * none-trivial without the history index. However, I am not sure how we can
 * leverage that delta in an interesting fashion for dynamic graphs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class BigdataGASEngine extends GASEngine {

//    private static final Logger log = Logger.getLogger(GASEngine.class);

    /**
     * Filter visits only edges (filters out attribute values).
     * <p>
     * Note: This filter is pushed down onto the AP and evaluated close to the
     * data.
     */
    static private final IElementFilter<ISPO> edgeOnlyFilter = new SPOFilter<ISPO>() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isValid(final Object e) {
            return ((ISPO) e).o().isURI();
        }
    };

    /**
     * The {@link IIndexManager} is used to access the graph.
     */
    private final IIndexManager indexManager;

    /**
     * 
     * @param indexManager
     *            The index manager.
     * @param nthreads
     *            The number of threads to use for the SCATTER and GATHER
     *            phases.
     * 
     *            TODO Scale-out: The {@link IIndexmanager} MAY be an
     *            {@link IBigdataFederation}. The {@link BigdataGASEngine} would
     *            automatically use remote indices. However, for proper
     *            scale-out we want to partition the work and the VS/ES so that
     *            would imply a different {@link IGASEngine} design.
     */
    public BigdataGASEngine(final IIndexManager indexManager, final int nthreads) {

        super(nthreads);
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;

    }

    @Override
    public <VS, ES, ST> IGASState<VS, ES, ST> newGASState(
            final IGraphAccessor graphAccessor,
            final IGASProgram<VS, ES, ST> gasProgram) {

        final IGASSchedulerImpl gasScheduler = newScheduler();

        return new BigdataGASState<VS, ES, ST>(
                (BigdataGraphAccessor) graphAccessor, gasScheduler, gasProgram);

    }

    @Override
    public <VS, ES, ST> IGASContext<VS, ES, ST> newGASContext(
            final IGraphAccessor graphAccessor,
            final IGASProgram<VS, ES, ST> gasProgram) {

        final BigdataGraphAccessor tmp = (BigdataGraphAccessor) graphAccessor;

        final IGASState<VS, ES, ST> gasState = newGASState(tmp, gasProgram);
        
        return new GASContext<VS, ES, ST>(this/* GASEngine */, tmp, gasState,
                gasProgram);

    }

    /**
     * 
     * 
     * @param namespace
     *            The namespace of the graph.
     * @param timestamp
     *            The timestamp of the view. If you specify
     *            {@link ITx#READ_COMMITTED}, then
     *            {@link IIndexManager#getLastCommitTime()} will be used to
     *            locate the most recently committed view of the graph at the
     *            start of each iteration. This provides one mechanism for
     *            dynamic graphs.
     * @return
     */
    public BigdataGraphAccessor newGraphAccessor(final String namespace,
            final long timestamp) {

        return new BigdataGraphAccessor(namespace, timestamp);
        
    }
    
    public class BigdataGraphAccessor implements IGraphAccessor {

        private final String namespace;
        private final long timestamp;
        private volatile WeakReference<AbstractTripleStore> kbRef;
        
        /**
         * 
         * @param namespace
         *            The namespace of the graph.
         * @param timestamp
         *            The timestamp of the view.
         */
        private BigdataGraphAccessor(final String namespace,final long timestamp) {
    
            this.namespace = namespace;
            this.timestamp = timestamp;
            
        }
        
        @Override
        public void advanceView() {

            if (this.timestamp == ITx.READ_COMMITTED) {

                /*
                 * Clear the reference. A new view will be obtained
                 * automatically by getKB().
                 */
                
                kbRef = null;

            }
            
        }

        /**
         * Return a view of the specified graph (aka KB) as of the specified
         * timestamp.
         * 
         * @return The graph.
         * 
         * @throws RuntimeException
         *             if the graph could not be resolved.
         */
        public AbstractTripleStore getKB() {

            if (kbRef == null) {

                synchronized (this) {

                    if (kbRef == null) {

                        kbRef = new WeakReference<AbstractTripleStore>(
                                resolveKB());

                    }

                }
            }

            return kbRef.get();

        }

        private AbstractTripleStore resolveKB() {

            long timestamp = this.timestamp;

            if (timestamp == ITx.READ_COMMITTED) {

                /**
                 * Note: This code is request the view as of the the last commit
                 * time. If we use ITx.READ_COMMITTED here then it will cause
                 * the Journal to provide us with a ReadCommittedIndex and that
                 * has a synchronization hot spot!
                 */

                timestamp = indexManager.getLastCommitTime();

            }

            final AbstractTripleStore kb = (AbstractTripleStore) indexManager
                    .getResourceLocator().locate(namespace, timestamp);

            if (kb == null) {

                throw new RuntimeException("Not found: namespace=" + namespace
                        + ", timestamp=" + TimestampUtility.toString(timestamp));

            }

            return kb;

        }

        public String getNamespace() {
         
            return namespace;
            
        }

        public Long getTimestamp() {
            
            return timestamp;
            
        }

        private final SPOKeyOrder getKeyOrder(final AbstractTripleStore kb,
                final boolean inEdges) {
            final SPOKeyOrder keyOrder;
            if (inEdges) {
                // in-edges: OSP / OCSP : [u] is the Object.
                keyOrder = kb.isQuads() ? SPOKeyOrder.OCSP : SPOKeyOrder.OSP;
            } else {
                // out-edges: SPO / (SPOC|SOPC) : [u] is the Subject.
                keyOrder = kb.isQuads() ? SPOKeyOrder.SPOC : SPOKeyOrder.SPO;
            }
            return keyOrder;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private Striterator<Iterator<ISPO>, ISPO> getEdges(
                final AbstractTripleStore kb, final boolean inEdges, final IV u) {

            final SPOKeyOrder keyOrder = getKeyOrder(kb, inEdges);

            final IIndex ndx = kb.getSPORelation().getIndex(keyOrder);

            final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

            keyBuilder.reset();

            IVUtility.encode(keyBuilder, u);

            final byte[] fromKey = keyBuilder.getKey();

            final byte[] toKey = SuccessorUtil.successor(fromKey.clone());

            return (Striterator<Iterator<ISPO>, ISPO>) new Striterator(
                    ndx.rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.DEFAULT,
                            ElementFilter.newInstance(edgeOnlyFilter)))
                    .addFilter(new Resolver() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        protected Object resolve(final Object e) {
                            final ITuple<ISPO> t = (ITuple<ISPO>) e;
                            return t.getObject();
                        }
                    });

        }

        /**
         * Return the edges for the vertex.
         * 
         * @param u
         *            The vertex.
         * @param edges
         *            Typesafe enumeration indicating which edges should be visited.
         * @return An iterator that will visit the edges for that vertex.
         */
        public ICloseableIterator<ISPO> getEdges(
                @SuppressWarnings("rawtypes") final IV u, final EdgesEnum edges) {

            final AbstractTripleStore kb = getKB();
            
            switch (edges) {
            case NoEdges:
                return new EmptyCloseableIterator<ISPO>();
            case InEdges:
                return (ICloseableIterator<ISPO>) getEdges(kb, true/* inEdges */, u);
            case OutEdges:
                return (ICloseableIterator<ISPO>) getEdges(kb, false/* inEdges */,
                        u);
            case AllEdges: {
                final Striterator<Iterator<ISPO>, ISPO> a = getEdges(kb,
                        true/* inEdges */, u);
                final Striterator<Iterator<ISPO>, ISPO> b = getEdges(kb,
                        false/* outEdges */, u);
                a.append(b);
                return (ICloseableIterator<ISPO>) a;
            }
            default:
                throw new UnsupportedOperationException(edges.name());
            }

        }

        // private IChunkedIterator<ISPO> getInEdges(final AbstractTripleStore kb,
        // final IV u) {
        //
        // // in-edges: OSP / OCSP : [u] is the Object.
        // return kb
        // .getSPORelation()
        // .getAccessPath(null/* s */, null/* p */, u/* o */, null/* c */,
        // edgeOnlyFilter).iterator();
        //
        // }
        //
        // private IChunkedIterator<ISPO> getOutEdges(final AbstractTripleStore kb,
        // final IV u) {
        //
        // // out-edges: SPO / SPOC : [u] is the Subject.
        // return kb
        // .getSPORelation()
        // .getAccessPath(u/* s */, null/* p */, null/* o */,
        // null/* c */, edgeOnlyFilter).iterator();
        //
        // }
        //
        // /**
        // * Return the edges for the vertex.
        // *
        // * @param u
        // * The vertex.
        // * @param edges
        // * Typesafe enumeration indicating which edges should be visited.
        // * @return An iterator that will visit the edges for that vertex.
        // *
        // * TODO There should be a means to specify a filter on the possible
        // * predicates to be used for traversal. If there is a single
        // * predicate, then that gives us S+P bound. If there are multiple
        // * predicates, then we have an IElementFilter on P (in addition to
        // * the filter that is removing the Literals from the scan).
        // *
        // * TODO Use the chunk parallelism? Explicit for(x : chunk)? This
        // * could make it easier to collect the edges into an array (but that
        // * is not required for powergraph).
        // */
        // @SuppressWarnings("unchecked")
        // private IChunkedIterator<ISPO> getEdges(final AbstractTripleStore kb,
        // final IV u, final EdgesEnum edges) {
        //
        // switch (edges) {
        // case NoEdges:
        // return new EmptyChunkedIterator<ISPO>(null/* keyOrder */);
        // case InEdges:
        // return getInEdges(kb, u);
        // case OutEdges:
        // return getOutEdges(kb, u);
        // case AllEdges:{
        // final IChunkedIterator<ISPO> a = getInEdges(kb, u);
        // final IChunkedIterator<ISPO> b = getOutEdges(kb, u);
        // final IChunkedIterator<ISPO> c = (IChunkedIterator<ISPO>) new
        // ChunkedStriterator<IChunkedIterator<ISPO>, ISPO>(
        // a).append(b);
        // return c;
        // }
        // default:
        // throw new UnsupportedOperationException(edges.name());
        // }
        //
        // }

    }

} // BigdataGASEngine
