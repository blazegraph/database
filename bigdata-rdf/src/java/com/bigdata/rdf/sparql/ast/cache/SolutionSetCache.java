/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
package com.bigdata.rdf.sparql.ast.cache;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.eval.IEvaluationContext;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rwstore.IPSOutputStream;
import com.bigdata.rwstore.IRWStrategy;
import com.bigdata.stream.Stream.StreamIndexMetadata;
import com.bigdata.striterator.CloseableIteratorWrapper;
import com.bigdata.striterator.ICloseableIterator;

/**
 * A cache for named SOLUTION SETS.
 * 
 * @see <a href="http://aksw.org/Projects/QueryCache"> Adaptive SPARQL Query
 *      Cache </a>
 * 
 * @see <a
 *      href="http://www.informatik.uni-leipzig.de/~auer/publication/caching.pdf
 *      > Improving the Performance of Semantic Web Applications with SPARQL
 *      Query Caching </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/524> SPARQL
 *      Query Cache </a>
 * 
 *      TODO Add support for declared and maintained views. Views would be
 *      declared using SPARQL queries. The view would listen for updates to
 *      statement patterns and invalidate/maintain the SPARQL result sets when a
 *      triple in a statement pattern in use by the query for that solution set
 *      has been added or removed.
 *      <p>
 *      Maintained views should be used transparently in queries where they can
 *      be incorporated by subsumption. General match of solution sets should be
 *      based on the hash code of the SPARQL query or the deep hash code of a
 *      normalized and optimized AST. Detailed match must be on either the query
 *      text or the AST (deep equals). AST based caching allows sub-select
 *      caching or even caching of sub-groups.
 *      <p>
 *      When BINDINGS are present, then the query solutions are not the same as
 *      when they are not present. This makes the cache somewhat more difficult
 *      to integration since the same query is not always the same (e.g.,
 *      include the hash of the exogenous solutions in the query hash code and
 *      we will get less reuse). Therefore, either the view must be computed
 *      without reference to a set of exogenous solutions or the exogenous
 *      solutions must be incorporated into the declaration of the view and
 *      considered when making decisions about subsumption.
 *      <p>
 *      Benchmark impact of cache on BSBM explore+update. The cache should be
 *      integrated into the query planner so we can cache solution sets for
 *      sub-groups and sub-selects.
 *      
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SolutionSetCache implements ISolutionSetCache {

    private static transient final Logger log = Logger
            .getLogger(SolutionSetCache.class);

    private final CacheConnectionImpl cache;

    /**
     * FIXME MVCC VIEWS: Convert over to Name2Addr and layered resolution
     * with appropriate concurrency control at each layer. The problem with
     * layering the MemStore journal mode over the RWStore journal mode is that
     * we wind up with two journals. There should be one journal with layered
     * addressing (that is, with a solution set cache in front of the Journal).
     * That might mean using a concurrent hash map for the resolution and named
     * locks to provide concurrency control. We could name the solution sets
     * within a scope:
     * 
     * <pre>
     * (cache|query|database)[.queryUUID].namespace[.joinVars]
     * </pre>
     * 
     * or something like that.
     * <p>
     * There are several problems here.
     * <p>
     * 1. We need a common semantics for visibility for the named solution sets
     * and the query and update operations. This cache can not provide that
     * without being somehow integrated with the MVCC architecture.
     * <p>
     * 2. We need to expire (at least some) cache objects. That expiration
     * should have a default but should also be configurable for each cache
     * object. The visibility issue also exists for expiration (we can not
     * expire a result set while it is being used).
     * <p>
     * 3. If we allow updates against named solution sets, then the visibility
     * of those updates must again be consistent with the MVCC architecture for
     * the query and update operations.
     * <p>
     * 4. We need to have metadata about solution sets on hand for explicit
     * CREATEs (e.g., supporting declared join variables).
     * 
     * FIXME Explicit recycling.
     * 
     * FIXME Appropriate synchronization, including for updates.
     * 
     * FIXME Life cycle hook for the cache. Destroy with the KB instance.
     * 
     */
    private final ConcurrentHashMap<String/* name */, SolutionSetStream> cacheMap;

    // private final ConcurrentWeakValueCacheWithTimeout<String/* name */,
    // IMemoryManager /* allocationContext */> cacheMap;

    private final String namespace;
    private final long timestamp;
    
    SolutionSetCache(final CacheConnectionImpl cache, final String namespace,
            final long timestamp) {

        if (cache == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        this.cache = cache;

        this.namespace = namespace;
        
        this.timestamp = timestamp;
        
        // /*
        // * TODO The expire should be per cached object, not global. We would
        // * need a different cache map class for that.
        // */
        // final long timeoutNanos = TimeUnit.SECONDS.toNanos(20);

        // this.cacheMap = new ConcurrentWeakValueCacheWithTimeout<String,
        // IMemoryManager>(
        // 0/* queueCapacity */, timeoutNanos);
        this.cacheMap = new ConcurrentHashMap<String, SolutionSetStream>();

    }

    /**
     * Return the backing store.
     * 
     * FIXME There is a reliance on the {@link IRWStrategy} right now because
     * the {@link IPSOutputStream} API has not yet been lifted onto the
     * {@link IRawStore} or a similar API.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/555" >
     *      Support PSOutputStream/InputStream at IRawStore </a>
     */
    private IRWStrategy getRWStrategy() {

        return (IRWStrategy) cache.getStore().getBufferStrategy();

    }

//    @Override
//    public void init() {
//        // NOP
//    }

    /**
     * Return the fully qualified name of the named solution set.
     * 
     * @param localName
     *            The local (aka application) name.
     *            
     * @return The fully qualified name.
     * 
     *         FIXME This does not support different index orders and will only
     *         when work for a {@link SolutionSetStream} (no components in the
     *         key). In order to support index orders we will need to modify the
     *         {@link ISolutionSetCache} API to accept the
     *         {@link INamedSolutionSetRef} rather than just the application
     *         name of the named solution set.
     */
    private String getFQN(final String localName) {
        
        return NamedSolutionSetRefUtility.getFQN(namespace, localName,
                IVariable.EMPTY/* joinVars */);

    }
    
    /*
     * FIXME Explicit close is probably not safe. We want to destroy to cached
     * solution sets if the AbstractTripleStore is destroyed, so that would be a
     * hook in AbstractResource#destroy() (or something near there).
     * 
     * FIXME The [cacheMap] MUST have a life cycle beyond the SparqlCacheImpl
     * view or we will discard the cached solutions as soon as this object goes
     * out of scope (simply by loosing the reference to the [cacheMap] - the
     * allocations would still be there on the backing InnerCacheJournal).
     */
    @Override
    public void close() {

        cacheMap.clear();

        // cacheStore.destroy();

    }

    @Override
    public void clearAllSolutions(final IEvaluationContext ctx) {

        final Iterator<Map.Entry<String, SolutionSetStream>> itr = cacheMap
                .entrySet().iterator();

        while (itr.hasNext()) {

            final Map.Entry<String, SolutionSetStream> e = itr.next();

            final String solutionSet = e.getKey();

            final SolutionSetStream sset = e.getValue();

            if (log.isInfoEnabled())
                log.info("solutionSet: " + solutionSet);

            sset.clear();

            itr.remove();

        }

    }

    @Override
    public boolean clearSolutions(final String solutionSet) {

        if (log.isInfoEnabled())
            log.info("solutionSet: " + solutionSet);

        final String fqn = getFQN(solutionSet);
        
        final SolutionSetStream sset = cacheMap.remove(fqn);

        if (sset != null) {
            sset.clear();

            return true;

        }

        return false;

    }

    public void putSolutions(final String solutionSet,
            final ICloseableIterator<IBindingSet[]> src) {

        if (src == null)
            throw new IllegalArgumentException();

        final String fqn = getFQN(solutionSet);

        SolutionSetStream sset = cacheMap.get(fqn);

        if (sset == null) {

            sset = _create(fqn, getDefaultMetadata());

        }

        // write out the solutions.
        writeSolutions(sset, src);

    }

    /**
     * Create iff it does not exist.
     * 
     * @param solutionSet
     *            The name.
     * @param params
     *            The configuration parameters.
     * @return A solution set with NOTHING written on it.
     * 
     *         TODO ISPO[] params is ignored (you can not configure for a BTree
     *         or HTree index for the solutions with a specified set of join
     *         variables for the index).
     */
    private SolutionSetStream _create(final String fqn,
            final ISPO[] params) {
        
        SolutionSetStream sset = cacheMap.get(fqn);

        if (sset != null)
            throw new RuntimeException("Exists: " + fqn);

        final StreamIndexMetadata md = new StreamIndexMetadata(fqn,
                UUID.randomUUID());

        sset = SolutionSetStream.create(getRWStrategy(), md);

        // sset = new SolutionSetMetadata(getStore(),
        // params == null ? getDefaultMetadata() : params, false/* readOnly */);

        cacheMap.put(fqn, sset);

        return sset;
    }

    public void createSolutions(final String solutionSet, final ISPO[] params) {

        final String fqn = getFQN(solutionSet);

        final SolutionSetStream sset = _create(fqn, params);

        /*
         * Write an empty solution set.
         */
        final List<IBindingSet[]> emptySolutionSet = new LinkedList<IBindingSet[]>();

        final ICloseableIterator<IBindingSet[]> src = new CloseableIteratorWrapper<IBindingSet[]>(
                emptySolutionSet.iterator());

        // write the solutions.
        writeSolutions(sset, src);

    }

    private void writeSolutions(final SolutionSetStream sset,
            final ICloseableIterator<IBindingSet[]> src) {

        sset.put(src);

    }

    public ISolutionSetStats getSolutionSetStats(final String solutionSet) {

        final String fqn = getFQN(solutionSet);

        final SolutionSetStream sset = cacheMap.get(fqn);

        if (sset != null) {

            return sset.getStats();

        }

        return null;

    }

    // /**
    // * Return the underlying index object for the named solution set.
    // *
    // * TODO There is a scalability problem with returning the
    // * {@link ISimpleIndexAccess} rather than an {@link ICloseableIterator}.
    // If
    // * the cache is remote or distributed, then we will not be able to return
    // an
    // * {@link ISimpleIndexAccess} interface. However, we can not return the
    // * iterator here since we need to attach the returned object to the
    // * {@link IQueryAttributes} for resolution at evaluation time by an
    // * operator. We probably need to return something that can be used to
    // obtain
    // * appropriate access to the solutions during the evaluation of an
    // operator
    // * - that is, we need one more level of indirection here.
    // * <p>
    // * The same mechanism for access to a remote solution stream could be used
    // * on a cluster when some solutions are on some node, or even distributed
    // * across some set of nodes. Maybe we even have some code already that
    // will
    // * work for this, such as the chunk message stuff.
    // */
    // public ISimpleIndexAccess getSolutionSet(final String solutionSet) {
    //
    // if (solutionSet == null)
    // throw new IllegalArgumentException();
    //
    // final SolutionSetStream sset = cacheMap.get(solutionSet);
    //
    // if (sset == null)
    // throw new IllegalStateException("Not found: " + solutionSet);
    //
    // return sset;
    //
    // }

    public ICloseableIterator<IBindingSet[]> getSolutions(
            final String solutionSet) {

        final String fqn = getFQN(solutionSet);

        final SolutionSetStream sset = cacheMap.get(fqn);

        if (sset == null)
            throw new IllegalStateException("Not found: " + solutionSet);

        // Return iterator over the decoded solutions.
        return sset.get();

    }

    public boolean existsSolutions(final String solutionSet) {

        if (solutionSet == null)
            throw new IllegalArgumentException();

        final SolutionSetStream sset = cacheMap.get(getFQN(solutionSet));

        return sset != null;

    }

    /**
     * Return the default metadata used when a named solution set is declared
     * implicitly rather than explicitly.
     * 
     * @return The metadata describing that solution set.
     * 
     *         TODO This is ignored and needs to be reconciled with
     *         {@link IndexMetadata}. However, we do want to provide this
     *         metadata in a CREATE schema as triples.
     */
    protected ISPO[] getDefaultMetadata() {

        return new ISPO[] {};

    }

} // SparqlCacheImpl