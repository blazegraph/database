/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.sparql.ast.ssets;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NamedSolutionSetRefUtility;
import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IBTreeManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.relation.AbstractResource;
import com.bigdata.stream.Stream.StreamIndexMetadata;
import com.bigdata.striterator.CloseableIteratorWrapper;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A manager for named SOLUTION SETS scoped by some namespace and timestamp.
 * 
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
 *      SPARQL Update </a>
 *      
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531"> SPARQL
 *      UPDATE for NAMED SOLUTION SETS </a>
 *      
 * @see <a href="http://aksw.org/Projects/QueryCache"> Adaptive SPARQL Query
 *      Cache </a>
 * 
 * @see <a
 *      href="http://www.informatik.uni-leipzig.de/~auer/publication/caching.pdf
 *      > Improving the Performance of Semantic Web Applications with SPARQL
 *      Query Caching </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/524"> SPARQL
 *      Query Cache </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
//*      Add support for declared and maintained views. Views would be
//*      declared using SPARQL queries. The view would listen for updates to
//*      statement patterns and invalidate/maintain the SPARQL result sets when a
//*      triple in a statement pattern in use by the query for that solution set
//*      has been added or removed.
//*      <p>
//*      Maintained views should be used transparently in queries where they can
//*      be incorporated by subsumption. General match of solution sets should be
//*      based on the hash code of the SPARQL query or the deep hash code of a
//*      normalized and optimized AST. Detailed match must be on either the query
//*      text or the AST (deep equals). AST based caching allows sub-select
//*      caching or even caching of sub-groups.
//*      <p>
//*      When BINDINGS are present, then the query solutions are not the same as
//*      when they are not present. This makes the cache somewhat more difficult
//*      to integration since the same query is not always the same (e.g.,
//*      include the hash of the exogenous solutions in the query hash code and
//*      we will get less reuse). Therefore, either the view must be computed
//*      without reference to a set of exogenous solutions or the exogenous
//*      solutions must be incorporated into the declaration of the view and
//*      considered when making decisions about subsumption.
//*      <p>
//*      Benchmark impact of cache on BSBM explore+update. The cache should be
//*      integrated into the query planner so we can cache solution sets for
//*      sub-groups and sub-selects.
public class SolutionSetManager implements ISolutionSetManager {

    private static transient final Logger log = Logger
            .getLogger(SolutionSetManager.class);

    /**
     * The backing store.
     */
    private final IBTreeManager store;

    /**
     * The backing store.
     */
    private IBTreeManager getStore() {

        return store;
        
    }
    
//    /**
//     * MVCC VIEWS: Convert over to Name2Addr and layered resolution with
//     * appropriate concurrency control at each layer.
//     * 
//     * <pre>
//     * (cache|query|database)[.queryUUID].namespace[.joinVars]
//     * </pre>
//     * 
//     * or something like that.
//     * <p>
//     * There are several problems here.
//     * <p>
//     * 1. We need a common semantics for visibility for the named solution sets
//     * and the query and update operations. This cache can not provide that
//     * without being somehow integrated with the MVCC architecture.
//     * <p>
//     * 2. We need to expire (at least some) cache objects. That expiration
//     * should have a default but should also be configurable for each cache
//     * object. The visibility issue also exists for expiration (we can not
//     * expire a result set while it is being used).
//     * <p>
//     * 3. If we allow updates against named solution sets, then the visibility
//     * of those updates must again be consistent with the MVCC architecture for
//     * the query and update operations.
//     * 
//     * Appropriate synchronization, including for updates, and on the
//     * Stream class (unisolated index protection).
//     * 
//     * This probably needs to buffer {@link Name2Addr} changes in a manner
//     * similar to {@link AbstractTask} in order to handle tx isolation
//     * correctly. However, this is not even hooked into the commit protocol so
//     * it is difficult to set how the view can evolve. I am unsure how this will
//     * play out for SPARQL UPDATE request sequences for either the UNISOLATED
//     * case or the case where the changes are isolated by a read/write tx.
//     * 
//     * The SolutionSetStream is not transaction aware at all. This makes
//     * it difficult to use named solution sets in SPARQL UPDATE in conjunction
//     * with full read-write transactions. There is no sense of a "FusedView" and
//     * the Stream is not being access through an AbstractTask running on the
//     * concurrency manager.
//     */
//    @Deprecated // We need to use Name2Addr and Name2Addr prefix scans
//    private final ConcurrentHashMap<String/* name */, SolutionSetStream> cacheMap;

    // private final ConcurrentWeakValueCacheWithTimeout<String/* name */,
    // IMemoryManager /* allocationContext */> cacheMap;

    private final String namespace;
    private final long timestamp;

    @Override
    public String toString() {

        return super.toString() + "{namespace=" + namespace + ",timestamp="
                + TimestampUtility.toString(timestamp) + "}";
    
    }
    
    public SolutionSetManager(final IBTreeManager store,
            final String namespace, final long timestamp) {

        if (store == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        this.store = store;

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
//        this.cacheMap = new ConcurrentHashMap<String, SolutionSetStream>();

    }

    @Override
    public void init() {
        
        // NOP
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Explicit close is not safe. We want to destroy to cached solution
     * sets if the AbstractTripleStore is destroyed. The hook is currently in
     * {@link AbstractResource#destroy()}.
     */
    @Override
    public void close() {

//        cacheMap.clear();

    }

    /**
     * Return the fully qualified name of the named solution set.
     * 
     * @param localName
     *            The local (aka application) name.
     * 
     * @return The fully qualified name.
     * 
     *         TODO Support different index orders. We can do a prefix scan on
     *         Name2Addr to find the existing index orders (but this is not
     *         currently efficient due to a bug in Name2Addr).
     *         <p>
     *         When reading out the {@link ISolutionSetStats} or the solutions
     *         themselves, all we need is the first such index that we find for
     *         a given namespace and localName.
     *         <p>
     *         When writing, we need to write on ALL such indices.
     *         <p>
     *         Either way, the scan should give us what we need.
     *         <p>
     *         For CREATE, the desired index order(s) should be declared in the
     *         create metadata.
     */
    private String getFQN(final String localName) {
        
        return NamedSolutionSetRefUtility.getFQN(namespace, localName,
                IVariable.EMPTY/* joinVars */);

    }

    private void assertNotReadOnly() {

        if (TimestampUtility.isReadOnly(timestamp)) {

            throw new UnsupportedOperationException("Read Only");

        }

    }
    
    @Override
    public void clearAllSolutions() {

        assertNotReadOnly();
        
        final String prefix = NamedSolutionSetRefUtility.getPrefix(namespace)
                .toString();

        if (log.isInfoEnabled())
            log.info("Scanning: prefix=" + prefix);
        
        final Iterator<String> itr = getStore().indexNameScan(prefix,
                timestamp);

        while(itr.hasNext()) {
            
            final String name = itr.next();
            
            getStore().dropIndex(name);

            if (log.isInfoEnabled())
                log.info("Dropping: " + name);
            
        }
        
//        final Iterator<Map.Entry<String, SolutionSetStream>> itr = cacheMap
//                .entrySet().iterator();
//
//        while (itr.hasNext()) {
//
//            final Map.Entry<String, SolutionSetStream> e = itr.next();
//
//            final String solutionSet = e.getKey();
//
//            final SolutionSetStream sset = e.getValue();
//
//            if (log.isInfoEnabled())
//                log.info("solutionSet: " + solutionSet);
//
//            sset.clear();
//
//            itr.remove();
//
//        }

    }

    /**
     * Return the named solution set.
     * 
     * @param fqn
     *            The fully qualified name.
     *            
     * @return The named solution set -or- <code>null</code> if it was not
     *         found.
     */
    private SolutionSetStream getSolutionSet(final String fqn) {

        if (timestamp == ITx.READ_COMMITTED) {

            return (SolutionSetStream) getStore().getIndexLocal(fqn,
                    getStore().getLastCommitTime());

        } else if (timestamp == ITx.UNISOLATED) {

            return (SolutionSetStream) getStore().getUnisolatedIndex(fqn);

        } else if(TimestampUtility.isReadWriteTx(timestamp)){

            final long ts;
            if (getStore() instanceof IJournal) {

                /*
                 * Optimized code path uses the readsOnCommitTime to improve
                 * caching.
                 */
                
                ts = ((IJournal) getStore())
                        .getLocalTransactionManager().getTx(timestamp)
                        .getReadsOnCommitTime();
                
            } else {

                /**
                 * Note: This code path is used by the TemporaryStore and
                 * possibly ResourceManager (for a data service). The [store]
                 * reference here is always the local index manager. Thus it can
                 * not be an IBigdataFederation.
                 * 
                 * TODO Use the readsOnCommitTime. Test coverage for
                 * TemporaryStore and DataService (but there is an open question
                 * about how to handle hash partitioned solution sets on a
                 * federation).
                 * 
                 * @see <a
                 *      href="https://sourceforge.net/apps/trac/bigdata/ticket/266">
                 *      Refactor native long tx id to thin object. </a>
                 */

                ts = timestamp;
                
            }

            return (SolutionSetStream) getStore().getIndexLocal(fqn, ts);

        } else {
            
            return (SolutionSetStream) getStore().getIndexLocal(fqn,
                    timestamp);

        }

        // Note: Forces all access to be unisolated.
//        return (SolutionSetStream) getStore().getUnisolatedIndex(fqn);
   
    }
    
    @Override
    public boolean existsSolutions(final String solutionSet) {

        if (solutionSet == null)
            throw new IllegalArgumentException();

        final SolutionSetStream sset = getSolutionSet(getFQN(solutionSet));

        return sset != null;

    }

    @Override
    public boolean clearSolutions(final String solutionSet) {

        if (log.isInfoEnabled())
            log.info("solutionSet: " + solutionSet);

        if (existsSolutions(solutionSet)) {

            final String fqn = getFQN(solutionSet);

            getStore().dropIndex(fqn);
        
            return true;
            
        }
        
//        final SolutionSetStream sset = cacheMap.remove(fqn);
//
//        if (sset != null) {
//            sset.clear();
//
//            return true;
//
//        }

        return false;

    }

    @Override
    public void putSolutions(final String solutionSet,
            final ICloseableIterator<IBindingSet[]> src) {

        if (src == null)
            throw new IllegalArgumentException();

        final String fqn = getFQN(solutionSet);

        SolutionSetStream sset = getSolutionSet(fqn);

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
        
        SolutionSetStream sset = getSolutionSet(fqn);

        if (sset != null)
            throw new RuntimeException("Exists: " + fqn);

        if (log.isInfoEnabled())
            log.info("Create: fqn=" + fqn + ", params="
                    + Arrays.toString(params));
        
        final StreamIndexMetadata md = new StreamIndexMetadata(fqn,
                UUID.randomUUID());
        
        /**
         * TODO GIST : We should not have to do this here. See
         * Checkpoint.create() and SolutionSetStream.create() for why this is
         * necessary.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/585 (GIST)
         */
        md.setStreamClassName(SolutionSetStream.class.getName());
        
        sset = (SolutionSetStream) getStore().register(fqn, md);

//        sset = SolutionSetStream.create(getRWStrategy(), md);

        // sset = new SolutionSetMetadata(getStore(),
        // params == null ? getDefaultMetadata() : params, false/* readOnly */);

//        cacheMap.put(fqn, sset);

        return sset;
    }

    @Override
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

    @Override
    public ISolutionSetStats getSolutionSetStats(final String solutionSet) {

        final String fqn = getFQN(solutionSet);

        final SolutionSetStream sset = getSolutionSet(fqn);

        if (sset != null) {

            final ISolutionSetStats stats = sset.getStats();

            if (stats == null)
                throw new RuntimeException("No statistics? solutionSet="
                        + solutionSet);

            return stats;
            
        }

        return null;

    }

    @Override
    public ICloseableIterator<IBindingSet[]> getSolutions(
            final String solutionSet) {

        final String fqn = getFQN(solutionSet);

        final SolutionSetStream sset = getSolutionSet(fqn);

        if (sset == null)
            throw new IllegalStateException("Not found: " + solutionSet);

        // Return iterator over the decoded solutions.
        return sset.get();

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

}
