package com.bigdata.rdf.sparql.ast.eval;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.query.Dataset;

import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.bop.cost.SubqueryCostReport;
import com.bigdata.bop.fed.FederatedQueryEngine;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.service.ResourceService;

/**
 * Helper class summarizes the named graphs or default graph mode for a quads
 * query.
 */
@SuppressWarnings("rawtypes")
public class DataSetSummary {

    public static Set<IV> toInternalValues(final Set<URI> graphs) {
		
        /*
         * Note: Per DAWG tests graph-02 and graph-04, a query against an empty
         * default graph collection or an empty named graph collection should
         * be constrained to NO graphs.  This is different from the case where
         * the dataset is simply not specified, which is interpreted as having
         * no constraint on the visited graphs.  If you uncomment the next two
         * lines, both graph-02 and graph-04 in the TCK will fail.
         */
//        if(graphs.isEmpty())
//            return null;
        
		final Set<IV> s = new LinkedHashSet<IV>();
		
		for (URI uri : graphs) {
			
			IV iv = null;
			
			if (uri != null && uri instanceof BigdataURI) {
				
				final BigdataURI bURI = (BigdataURI) uri;
				
				iv = bURI.getIV();
				
			}
			
			s.add(iv);
			
		}
		
		return s;
		
	}
	
    /**
     * The set of graphs. The {@link URI}s MUST have been resolved against the
     * appropriate {@link LexiconRelation} such that their term identifiers
     * (when the exist) are known. If any term identifier is
     * {@link IRawTripleStore#NULL}, then the corresponding graph does not exist
     * and no access path will be queried for that graph. However, a non-
     * {@link IRawTripleStore#NULL} term identifier may also identify a graph
     * which does not exist, in which case an access path will be created for
     * that {@link URI}s but will not visit any data.
     */
//    public final Iterable<? extends URI> graphs;
	public final Set<IV> graphs;

    /**
     * The #of graphs in {@link #graphs} whose term identifier is known. While
     * this is not proof that there is data in the quad store for a graph having
     * the corresponding {@link URI}, it does allow the possibility that a graph
     * could exist for that {@link URI}.
     */
    public final int nknown;

    /**
     * The #of graphs in {@link #graphs} whose term identifier is not known. For
     * QUERY, this is proof that there is no data in the quad store for a graph
     * having the corresponding {@link URI}. However, for UPDATE it is possible
     * that a graph could be created for that {@link URI} during an UPDATE
     * operation.
     */
    public final int nunknown;

    /**
     * The {@link IV} for the first graph having a known {@link IV} and
     * {@link IRawTripleStore#NULL} if no graphs were specified having a known
     * {@link IV}.
     */
    public final IV firstContext;

    /**
     * 
     * @param graphs
     *            The set of named graphs in the SPARQL DATASET (optional). A
     *            runtime exception will be thrown during evaluation of the if
     *            the {@link URI}s are not {@link BigdataURI}s. If
     *            <code>graphs := null</code>, then the set of named graphs is
     *            understood to be ALL graphs in the quad store.
     * @param update
     *            When <code>true</code>, unknown {@link IV}s WILL NOT be pruned
     *            from the {@link DataSetSummary}. This is because those graphs
     *            might be implicitly created during an UPDATE operation.
     */
    public DataSetSummary(final Set<IV> graphs, final boolean update) {

        IV firstContext = null;

        if (graphs == null) {

            nknown = Integer.MAX_VALUE;
            nunknown = Integer.MAX_VALUE;

        } else {

            final Iterator<IV> itr = graphs.iterator();

            int nknown = 0;
            int nunknown = 0;

            while (itr.hasNext()) {

                final IV iv = itr.next();

                if (iv == null)
                    continue;

                if (iv.isNullIV()) {

                    nunknown++;

                } else {

                    if (++nknown == 1) {

                        firstContext = iv;

                    }

                }

            } // while

            this.nknown = nknown;
            this.nunknown = nunknown;

        }

        this.firstContext = firstContext;

        /*
         * Note: Includes unknown IVs iff [update:=true].
         */
        final IV[] a = new IV[nknown + (update ? nunknown : 0)];

        final Iterator<IV> itr = graphs.iterator();

        int nknown = 0;

        while (itr.hasNext()) {

            final IV iv = itr.next();

            if (iv == null)
                continue;

            if (iv.isNullIV() && !update) {
                // Drop unknown IVs unless [update:=true].
                continue;
            }

            a[nknown++] = iv;

        } // while

        /*
         * Put the graphs into termId order. Since the individual access paths
         * will be formed by binding [c] to each graphId in turn, evaluating
         * those access paths in graphId order will make better use of the
         * B+Tree cache as the reads will tend to be more clustered.
         */
        Arrays.sort(a);

        // Populate hash set which will maintain the sorted order.
        this.graphs = new LinkedHashSet<IV>(nknown);

        for (int i = 0; i < nknown; i++) {

            this.graphs.add(a[i]);

        }

    }

    /**
     * Return the distinct {@link IV}s for the graphs known to the database.
     * 
     * @return An ordered set of the distinct {@link IV}s.
     */
    public Set<IV> getGraphs() {

        return graphs != null ? graphs : Collections.<IV> emptySet();

    }

    /**
     * Estimate cost of SUBQUERY with C bound (sampling).
     * 
     * @param context
     * @param limit
     *            The maximum #of samples to take.
     * @param pred
     *            The predicate.
     * 
     * @return The estimated cost report. This is adjusted based on the sample
     *         size and the #of graphs against which the query was issued and
     *         represents the total expected cost of the subqueries against all
     *         of the graphs in the {@link Dataset}.
     * 
     * @todo Subquery will be less efficient than a scan when the access path is
     *       remote since there will be remote requests. This model does not
     *       capture that additional overhead. We need to measure the overhead
     *       using appropriate data sets and queries and then build it into the
     *       model. The overhead itself could be changed dramatically by
     *       optimizations in the {@link FederatedQueryEngine} and the
     *       {@link ResourceService}.
     * 
     * @todo This should randomly sample in case there is bias.
     */
    @SuppressWarnings({ "unchecked" })
    public SubqueryCostReport estimateSubqueryCost(
            final BOpContextBase context, final int limit, final Predicate pred) {

        final IRelation r = context.getRelation(pred);

        double subqueryCost = 0d;

        long rangeCount = 0L;

        int nsamples = 0;

//        for (URI uri : graphs) {
        for (IV graph : graphs) {

            if (nsamples == limit)
                break;

//            final IV graph = ((BigdataURI) uri).getIV();
//
//            if (graph == null)
//                continue;

            final Predicate tmp = pred.asBound((IVariable) pred.get(3),
                    new Constant(graph));

            final AccessPath ap = (AccessPath) context.getAccessPath(r, tmp);

            subqueryCost += ap.estimateCost().cost;

            rangeCount += context.getAccessPath(context.getRelation(tmp), tmp)
                    .rangeCount(false/* exact */);

            nsamples++;

        }

        subqueryCost = (subqueryCost * nknown) / nsamples;

        rangeCount = (rangeCount * nknown) / nsamples;

        return new SubqueryCostReport(nknown, limit, nsamples, rangeCount,
                subqueryCost);

    }

    @Override
    public String toString() {

        return "DataSetSummary{ngraphs=" + graphs.size() + ", nknown=" + nknown
                + ", nunknown=" + nunknown + ", graphs=" + graphs + "}"
                ;
        
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o)
            return true;
        
        if (!(o instanceof DataSetSummary))
            return false;
        
        final DataSetSummary t = (DataSetSummary) o;
        
        if (!graphs.equals(t.graphs))
            return false;

        return true;
        
    }
    
}
