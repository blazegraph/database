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
/*
 * Created on Aug 15, 2012
 */
package com.bigdata.bop;

import java.util.Arrays;
import java.util.UUID;

import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.solutions.ISolutionSet;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ISimpleIndexAccess;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IBTreeManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rdf.sparql.ast.ISolutionSetStats;
import com.bigdata.rdf.sparql.ast.ssets.ISolutionSetManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.Chunkerator;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Utility class for {@link INamedSolutionSetRef}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NamedSolutionSetRefUtility {

    /**
	 * Factory for {@link INamedSolutionSetRef}s that will be resolved against
	 * the {@link IRunningQuery} identified by the specified <i>queryId</i>.
	 * 
	 * @param queryId
	 *            The {@link UUID} of the {@link IRunningQuery} where you need
	 *            to look to find the data (optional). When <code>null</code>,
	 *            you must look at the current query. When non-<code>null</code>
	 *            you must look at the specified query. See BLZG-1493.
	 * @param namedSet
	 *            The application level name for the named solution set
	 *            (required).
	 * @param joinVars
	 *            The join variables (required, but may be an empty array).
	 *            
	 * @see <a href="https://jira.blazegraph.com/browse/BLZG-1493" > NPE in
	 *      nested star property paths </a>
	 */
    @SuppressWarnings("rawtypes")
    public static INamedSolutionSetRef newInstance(//
            final UUID queryId, //
            final String namedSet,//
            final IVariable[] joinVars//
    ) {

        // Note: checked by the constructor.

//        if (queryId == null)
//            throw new IllegalArgumentException();
//
//        if (namedSet == null)
//            throw new IllegalArgumentException();
//
//        if (joinVars == null)
//            throw new IllegalArgumentException();

        return new NamedSolutionSetRef(queryId, namedSet, joinVars);

    }
    
    /**
     * Factory for {@link INamedSolutionSetRef}s that will be resolved against a
     * KB view identified by a <i>namespace</i> and <i>timestamp</i>.
     * 
     * @param namespace
     *            The bigdata namespace of the {@link AbstractTripleStore} where
     *            you need to look to find the data (required).
     * @param timestamp
     *            The timestamp of the view.
     * @param localName
     *            The application level name for the named solution set
     *            (required).
     * @param joinVars
     *            The join variables (required, but may be an empty array).
     */
    @SuppressWarnings("rawtypes")
    public static INamedSolutionSetRef newInstance(//
            final String namespace, //
            final long timestamp,//
            final String localName,//
            final IVariable[] joinVars//
    ) {

        // Note: checked by the constructor.
        
//        if (namespace == null)
//            throw new IllegalArgumentException();
//
//        if (namedSet == null)
//            throw new IllegalArgumentException();
//
//        if (joinVars == null)
//            throw new IllegalArgumentException();

        return new NamedSolutionSetRef(namespace, timestamp, localName,
                joinVars);

    }
    
    /**
     * Parses the {@link INamedSolutionSetRef#toString()} representation,
     * returning an instance of that interface.
     * 
     * @see NamedSolutionSetRef#toString()
     */
    public static INamedSolutionSetRef valueOf(final String s) {

        final String namedSet;
        {

            final int posNamedSet = assertIndex(s, s.indexOf("localName="));
            
            final int posNamedSetEnd = assertIndex(s,
                    s.indexOf(",", posNamedSet));
            
            namedSet = s.substring(posNamedSet + 10, posNamedSetEnd);
        
        }

        @SuppressWarnings("rawtypes")
        final IVariable[] joinVars;
        {
        
            final int posJoinVars = assertIndex(s, s.indexOf("joinVars=["));
            
            final int posJoinVarsEnd = assertIndex(s,
                    s.indexOf("]", posJoinVars));
            
            final String joinVarsStr = s.substring(posJoinVars + 10,
                    posJoinVarsEnd);

            final String[] a = joinVarsStr.split(", ");

            joinVars = new IVariable[a.length];

            for (int i = 0; i < a.length; i++) {

                joinVars[i] = Var.var(a[i]);

            }
        }
        
        if (s.indexOf("queryId") != -1) {

            final int posQueryId = assertIndex(s, s.indexOf("queryId="));
            final int posQueryIdEnd = assertIndex(s, s.indexOf(",", posQueryId));
            final String queryIdStr = s.substring(posQueryId + 8, posQueryIdEnd);
            final UUID queryId = UUID.fromString(queryIdStr);

            return NamedSolutionSetRefUtility.newInstance(queryId, namedSet, joinVars);

        } else {

            final String namespace;
            {
                final int posNamespace = assertIndex(s, s.indexOf("namespace="));
                final int posNamespaceEnd = assertIndex(s,
                        s.indexOf(",", posNamespace));
                namespace = s.substring(posNamespace + 10, posNamespaceEnd);
            }

            final long timestamp;
            {
                final int posTimestamp = assertIndex(s, s.indexOf("timestamp="));
                final int posTimestampEnd = assertIndex(s,
                        s.indexOf(",", posTimestamp));
                final String timestampStr = s.substring(posTimestamp + 10,
                        posTimestampEnd);
                timestamp = Long.valueOf(timestampStr);
            }

            return NamedSolutionSetRefUtility.newInstance(namespace, timestamp,
                    namedSet, joinVars);

        }

    }

    static private int assertIndex(final String s, final int index) {

        if (index >= 0)
            return index;
        
        throw new IllegalArgumentException(s);
        
    }

    /**
     * Return the fully qualified name for a named solution set NOT attached to
     * a query.
     * <p>
     * Note: this includes the namespace (to keep the named solution sets
     * distinct for different KB instances) and the ordered list of key
     * components (so we can identify different index orders for the same
     * solution set).
     * <p>
     * Note: This does not allow duplicate indices of different types (BTree
     * versus HTree) for the same key orders as their FQNs would collide.
     * <p>
     * Note: All index orders for the same "namedSet" will share a common
     * prefix.
     * <P>
     * Note: All named solution set for the same KB will share a common prefix,
     * and that prefix will be distinct from any other index.
     * 
     * @param namespace
     *            The KB namespace.
     * @param localName
     *            The local (aka application) name for the named solution set.
     * @param joinVars
     *            The ordered set of key components (differentiates among
     *            different indices for the same named solution set).
     * 
     * @return The fully qualified name.
     */
    public static String getFQN(//
            final String namespace,//
            final String localName, //
            final IVariable[] joinVars//
            ) {

        if (namespace == null)
            throw new IllegalArgumentException();

        if (localName == null)
            throw new IllegalArgumentException();

        if (joinVars == null)
            throw new IllegalArgumentException();

        final StringBuilder sb = getPrefix(namespace, localName);
        
        if (joinVars.length != 0)
            sb.append(".");
        
        boolean first = true;
        
        for (IVariable<?> v : joinVars) {
        
            if (first) {
                
                first = false;
                
            } else {
            
                sb.append("-");
                
            }
            
            sb.append(v.getName());
        }

        return sb.toString();

    }

    /**
     * The prefix that may be used to identify all named solution sets belonging
     * to the specified KB namespace.
     * 
     * @param namespace
     *            The KB namespace.
     * 
     * @return The prefix shared by all solution sets for that KB namespace.
     */
    public static StringBuilder getPrefix(final String namespace) {

        final StringBuilder sb = new StringBuilder(96);

        sb.append(namespace);

        sb.append(".solutionSets");

        return sb;

    }

    /**
     * The prefix that may be used to identify all named solution sets belonging
     * to the specified KB namespace and having the specified localName. This
     * may be used to find the different indices over the same named solution
     * set when there is more than one index order for that named solution set.
     * 
     * @param namespace
     *            The KB namespace.
     * @param localName
     *            The application name for the named solution set.
     * 
     * @return The prefix shared by all solution sets for that KB namespace and
     *         localName.
     */
    public static StringBuilder getPrefix(final String namespace,
            final String localName) {

        final StringBuilder sb = getPrefix(namespace);

        sb.append(".");

        sb.append(localName);

        return sb;

    }
    
//    /**
//     * Resolve the pre-existing named solution set returning its
//     * {@link ISolutionSetStats}.
//     * 
//     * @param sparqlCache
//     * @param localIndexManager
//     * @return The {@link ISolutionSetStats}
//     * 
//     * @throws RuntimeException
//     *             if the named solution set can not be found.
//     */
//    public static ISolutionSetStats getSolutionSetStats(
//            final ISparqlCache sparqlCache,//
//            final AbstractJournal localIndexManager, //
//            final INamedSolutionSetRef namedRef) {
//        
//        return getSolutionSetStats(sparqlCache, localIndexManager,
//                namedRef.getNamespace(), namedRef.getTimestamp(),
//                namedRef.getLocalName(), namedRef.getJoinVars());
//
//    }
//    
//    /**
//     * Resolve the pre-existing named solution set returning an iterator that
//     * will visit the solutions (access path scan).
//     * 
//     * @return An iterator that will visit the solutions in the named solution
//     *         set.
//     * @throws RuntimeException
//     *             if the named solution set can not be found.
//     */
//    public static ICloseableIterator<IBindingSet[]> getSolutionSet(
//            final ISparqlCache sparqlCache,//
//            final AbstractJournal localIndexManager,//
//            final INamedSolutionSetRef namedRef,//
//            final int chunkCapacity//
//    ) {
//
//        return getSolutionSet(sparqlCache, localIndexManager,
//                namedRef.getNamespace(), namedRef.getTimestamp(),
//                namedRef.getLocalName(), namedRef.getJoinVars(), chunkCapacity);
//        
//    }
    
    /**
     * Resolve the pre-existing named solution set returning its
     * {@link ISolutionSetStats}.
     * 
     * @param sparqlCache
     * @param localIndexManager
     * @param namespace
     * @param timestamp
     * @param localName
     * @param joinVars
     * @return The {@link ISolutionSetStats}
     * 
     * @throws RuntimeException
     *             if the named solution set can not be found.
     * 
     *             FIXME Drop joinVars here and just do a Name2Addr scan on the
     *             value returned by {@link #getPrefix(String, String)} to see
     *             if we can locate an index (regardless of the join variables).
     *             It does not matter *which* index we find, as long as it is
     *             the same data.
     */
    public static ISolutionSetStats getSolutionSetStats(//
            final ISolutionSetManager sparqlCache,//
            final IBTreeManager localIndexManager, //
            final String namespace,//
            final long timestamp,//
            final String localName,//
            final IVariable[] joinVars//
            ) {

        if (localName == null)
            throw new IllegalArgumentException();

        if (sparqlCache != null) {

            final ISolutionSetStats stats = sparqlCache
                    .getSolutionSetStats(localName);

            if (stats != null) {

                return stats;

            }

        }

        final String fqn = getFQN(namespace, localName, joinVars);

        final AbstractJournal localJournal = (AbstractJournal) localIndexManager;

        final ISimpleIndexAccess index;

        if (timestamp == ITx.UNISOLATED) {

            /*
             * FIXME We may need to wrap this with the lock provided by
             * UnisolatedReadWriteIndex.
             * 
             * TODO A read-committed view would be Ok here (as long as
             * the data were committed and not written on by the current
             * SPARQL UPDATE request).
             */
            index = localJournal.getUnisolatedIndex(fqn);

        } else if(TimestampUtility.isReadWriteTx(timestamp)) {
            
            final long readsOnCommitTime = localJournal
                    .getLocalTransactionManager().getTx(timestamp)
                    .getReadsOnCommitTime();

            index = localJournal.getIndexLocal(fqn, readsOnCommitTime);

        } else if (TimestampUtility.isReadOnly(timestamp)) {

            index = localJournal.getIndexLocal(fqn, timestamp);

        } else {
            
            index = null;
            
        }

        if (index == null)
            throw new RuntimeException("Unresolved solution set: namespace="
                    + namespace + ", timestamp=" + timestamp + ", localName="
                    + localName + ", joinVars=" + Arrays.toString(joinVars));

        return ((ISolutionSet)index).getStats();

    }

    /**
     * Resolve the pre-existing named solution set returning an iterator that
     * will visit the solutions (access path scan).
     * <p>
     * This method MUST NOT be used if the named solution set is hung off of an
     * {@link IRunningQuery}. In that case, you need to resolve the
     * {@link IRunningQuery} using {@link INamedSolutionSetRef#getQueryId()} and
     * then resolve the solution set on the {@link IQueryAttributes} associated
     * with that {@link IRunningQuery}.
     * 
     * @return An iterator that will visit the solutions in the named solution
     *         set.
     * @throws RuntimeException
     *             if the named solution set can not be found.
     * 
     *             FIXME Drop joinVars here and just do a Name2Addr scan on the
     *             value returned by {@link #getPrefix(String, String)} to see
     *             if we can locate an index (regardless of the join variables).
     *             It does not matter *which* index we find, as long as it is
     *             the same data.
     * 
     *             TODO Provide federation-wide access to a durable named index?
     *             The concept would need to be developed further. Would this be
     *             a local index exposed to other nodes in the federation? A
     *             hash partitioned index? An remote view of a global
     *             {@link IIndex}?
     */
    public static ICloseableIterator<IBindingSet[]> getSolutionSet(
            final ISolutionSetManager sparqlCache,//
            final IBTreeManager localIndexManager,//
            final String namespace,//
            final long timestamp,//
            final String localName,//
            final IVariable[] joinVars,//
            final int chunkCapacity//
    ) {

        /*
         * We will now look for an index (BTree, HTree, Stream, etc) having
         * Fully Qualified Name associated with this reference.
         * 
         * The search order is CACHE, local Journal, federation.
         * 
         * TODO We might need/want to explicitly identify the conceptual
         * location of the named solution set (cache, local index manager,
         * federation) when the query is compiled so we only look in the right
         * place at when the operator is executing. That could decrease latency
         * for operators which execute multiple times, report errors early if
         * something can not be resolved, and eliminate some overhead with
         * testing remote services during operator evaluation (if the cache is
         * non-local).
         */

        if (sparqlCache != null && sparqlCache.existsSolutions(localName)) {

            return sparqlCache.getSolutions(localName);

        }

        final String fqn = getFQN(namespace, localName, joinVars);

        final AbstractJournal localJournal = (AbstractJournal) localIndexManager;

        final ISimpleIndexAccess index;

        if (timestamp == ITx.UNISOLATED) {

            /*
             * FIXME We may need to wrap this with the lock provided by
             * UnisolatedReadWriteIndex.
             */
            index = localJournal.getUnisolatedIndex(fqn);

        } else if (TimestampUtility.isReadOnly(timestamp)) {

            index = localJournal.getIndexLocal(fqn, timestamp);

        } else {

            /*
             * Note: This is here to catch assumptions about the timestamp. For
             * example, we might see read/write txIds here. That could be Ok,
             * but it needs to be handled correctly.
             */

            throw new AssertionError("localName=" + localName);

        }

        if (index == null)
            throw new RuntimeException("Unresolved solution set: namespace="
                    + namespace + ", timestamp=" + timestamp + ", localName="
                    + localName + ", joinVars=" + Arrays.toString(joinVars));

        // Iterator visiting the solution set.
        @SuppressWarnings("unchecked")
        final ICloseableIterator<IBindingSet> src = (ICloseableIterator<IBindingSet>) index
                .scan();

        return new Chunkerator<IBindingSet>(src, chunkCapacity,
                IBindingSet.class);

    }

}
