package com.bigdata.rdf.graph.impl.bd;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.Tuple;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Utility class for operations on the backing graph (sampling and the like).
 * These operations are specific to the bigdata backend (they are index and
 * index manager aware).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Add a utility to find the vertex with the maximum degree. That
 *         requires a scan on SPO (or a distinct term advancer that computes the
 *         range count of the skipped interval).
 */
public class BigdataGASUtil {

    private static final Logger log = Logger.getLogger(BigdataGASUtil.class);

    /**
     * Return a sample (without duplicates) of vertices from the graph.
     * <p>
     * Note: This sampling procedure has a bias in favor of the vertices with
     * the most edges and properties (vertices are choosen randomly in
     * proportion to the #of edges and properties for the vertex).
     * 
     * @param desiredSampleSize
     *            The desired sample size.
     * 
     * @return The distinct samples that were found.
     */
    @SuppressWarnings("rawtypes")
    static public IV[] getRandomSample(final Random r,
            final AbstractTripleStore kb, final int desiredSampleSize) {
    
//        /*
//         * TODO This assumes a local, non-sharded index. The specific approach
//         * to identifying a starting vertex relies on the ILinearList API. If
//         * the caller is specifying the starting vertex then we do not need to
//         * do this.
//         * 
//         * TODO The bias here is towards vertices having more out-edges and/or
//         * attributes since the sample is uniform over the triples in the index
//         * and a triple may be either an edge or an attribute value (or a link
//         * attribute using RDR).
//         */
//        final BTree ndx = (BTree) kb.getSPORelation().getPrimaryIndex();
//    
//        // Truncate at MAX_INT.
//        final int size = (int) Math.min(ndx.rangeCount(), Integer.MAX_VALUE);
    
        // Maximum number of samples to attempt.
        final int limit = (int) Math.min(desiredSampleSize * 3L,
                Integer.MAX_VALUE);
    
        final Set<IV> samples = new HashSet<IV>();
    
        int round = 0;
        while (samples.size() < desiredSampleSize && round++ < limit) {
    
            final IV iv = BigdataGASUtil.getRandomVertex(r, kb);
    
            samples.add(iv);
    
        }
    
        return samples.toArray(new IV[samples.size()]);
        
    }

    /**
     * Return a random vertex.
     * 
     * @param kb
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static IV getRandomVertex(final Random r, final AbstractTripleStore kb) {
    
        /*
         * TODO This assumes a local, non-sharded index. The specific
         * approach to identifying a starting vertex relies on the
         * ILinearList API. If the caller is specifying the starting vertex
         * then we do not need to do this.
         * 
         * TODO The bias here is towards vertices having more out-edges
         * and/or attributes since the sample is uniform over the triples in
         * the index and a triple may be either an edge or an attribute
         * value (or a link attribute using RDR).
         */
        final BTree ndx = (BTree) kb.getSPORelation().getPrimaryIndex();

        // Select a random starting vertex.
        IV startingVertex = null;
        {

            // Truncate at MAX_INT.
            final int size = (int) Math
                    .min(ndx.rangeCount(), Integer.MAX_VALUE);
    
            while (size > 0L && startingVertex == null) {
    
                final int rindex = r.nextInt(size);
    
                /*
                 * Use tuple that will return both the key and the value so we
                 * can decode the entire tuple.
                 */
                final Tuple<ISPO> tuple = new Tuple<ISPO>(ndx, IRangeQuery.KEYS
                        | IRangeQuery.VALS);
    
                if (ndx.valueAt(rindex, tuple) == null) {
    
                    /*
                     * This is a deleted tuple. Try again.
                     * 
                     * Note: This retry is NOT safe for production use. The
                     * index might not have any undeleted tuples. However, we
                     * should not be using an index without any undeleted tuples
                     * for a test harness, so this should be safe enough here.
                     * If you want to use this production, at a mimimum make
                     * sure that you limit the #of attempts for the outer loop.
                     */
                    continue;
    
                }
    
                // Decode the selected edge.
                final ISPO edge = (ISPO) ndx.getIndexMetadata()
                        .getTupleSerializer().deserialize(tuple);
    
                // Use the subject for that edge as the starting vertex.
                startingVertex = edge.s();
    
                if (log.isInfoEnabled())
                    log.info("Starting vertex: " + startingVertex);
    
            }
    
        }
    
        if (startingVertex == null)
            throw new RuntimeException("No starting vertex: nedges="
                    + ndx.rangeCount());

        return startingVertex;
    
    }

}
