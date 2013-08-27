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
package com.bigdata.rdf.graph.analytics;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.Tuple;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.graph.impl.bd.BigdataGASUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Test class for BFS traversal.
 * <p>
 * Breadth First Search (BFS) is an iterative graph traversal primitive. A
 * frontier contains all vertices that have not yet been expanded - these are
 * the active vertices and will be visited in the next iteration. For each
 * vertex in the frontier, the edges are traversed one step (edges may be
 * directed or undirected). This expansion step gives us a new frontier from
 * which we may remove all vertices that have already been visited. If no new
 * vertices are discovered, then the traversal is finished.
 * <p>
 * It is also possible to use a strategy without a barrier at the end of an
 * iteration. However, this is not BFS. While this approach could be faster
 * since you could schedule the resolution of new vertices immediately, it lacks
 * the ability to incorporate an iteration number into the computation and
 * (without automatic chunking) would not be able to order the IOs for the index
 * reads, which would cause a substiantial penalty when using disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @deprecated This was a trial implementation. Run time is ~9600ms on an
 *             airbook. The generic GAS BFS implementation is a bit closer to
 *             10s (10000ms) on the same machine. This class can be discarded
 *             once we start optimization of the GASEngine implementation.
 */
public class TestBFS0 {

    private static final Logger log = Logger.getLogger(TestBFS.class);
    
    private static class BFS implements Runnable {

        // Note: could be scale-out with remote indices or running embedded
        // inside of a HA server.
        private final AbstractTripleStore kb;
        
        /**
         * The set of distinct vertices that have been visited.
         * 
         * TODO Could be a Map if we wanted to associate state with each visited
         * vertex. Could be an HTree if we wanted a very scalable data
         * structure. For BFS, we do not need to track anything else. Should be
         * a concurrent data structure if we wind up striping the reads on the
         * index into differet key-range runs or queuing the index reads for
         * each page and then servicing them concurrently when those pages are
         * delievered.
         */
        private final ConcurrentHashSet<IV> visited = new ConcurrentHashSet<IV>();

        /**
         * The set of vertices that were identified in the current iteration.
         */
        private final Set<IV>[] frontier = new HashSet[2];
        
        private final AtomicInteger round = new AtomicInteger(0);

        /**
         * The current frontier.
         */
        protected Set<IV> frontier() {

            return frontier[round.get() % 2];
            
        }
        
        /**
         * The new frontier - this is populated during the round. At the end of
         * the round, the new frontier replaces the current frontier (this
         * happens when we increment the {@link #round()}). If the current
         * frontier is empty after that replacement, then the traversal is done.
         */
        protected Set<IV> newFrontier() {

            return frontier[(round.get() + 1) % 2];

        }

        /**
         * The current round.
         */
        protected int round() {

            return round.get();
            
        }

        /**
         * Filter visits only edges (filters out attribute values).
         * <p>
         * Note: This filter is pushed down onto the AP and evaluated close to
         * the data. 
         * 
         * TODO Lift out as static utility class.
         */
        private final IElementFilter<ISPO> filter = new SPOFilter<ISPO>() {
            private static final long serialVersionUID = 1L;
            @Override
            public boolean isValid(final Object e) {
                return ((ISPO)e).o().isURI();
            }
        }; 

        /**
         * 
         * @param kb
         *            The graph.
         * @param startingVertex
         *            The starting vertex for the traversal.
         */
        public BFS(final AbstractTripleStore kb, 
                final IV startingVertex) {

            if(kb == null)
                throw new IllegalArgumentException();

            if(startingVertex == null)
                throw new IllegalArgumentException();
            
            this.kb = kb;

            this.visited.add(startingVertex);

            this.frontier[0] = new HashSet<IV>();

            this.frontier[1] = new HashSet<IV>();
            
            frontier().add(startingVertex);
            
        }
        
        @Override
        public void run() {
            
            while (!frontier().isEmpty()) {
                
                doRound();
                
                // Swaps old and new frontiers.
                round.incrementAndGet();
                
            }
            
            if (log.isInfoEnabled())
                log.info("Done: round=" + round());

        }
        
        /*
         * Note: Must await barrier if concurrent threads.
         * 
         * TODO This is a single thread.
         */
        private void doRound() {

            /*
             * Generate an ordered frontier to maximize the IO efficiency of the
             * B+Tree.
             */
            final IV[] f;
            {
                
                final int size = frontier().size();

                if (log.isInfoEnabled())
                    log.info("Round=" + round + ", frontierSize=" + size
                            + ", visitedSize=" + visited.size());

                frontier().toArray(f = new IV[size]);

                /*
                 * Order for index access. An ordered scan on a B+Tree is 10X
                 * faster than random access lookups.
                 * 
                 * Note: This uses natural IV order, which is also the index
                 * order.
                 */
                java.util.Arrays.sort(f);
                
            }

            /*
             * This is the new frontier. It is initially empty. All newly
             * discovered vertices are inserted into this frontier.
             */
            final Set<IV> ftp1 = newFrontier();
            ftp1.clear(); // clear() may be expensive.
            
            /*
             * For all vertices in the frontier.
             * 
             * TODO Could stripe scan using multiple threads (or implement
             * parallel iterator on B+Tree).
             */
            for(IV v : f) {

                /*
                 * All out-edges for that vertex.
                 * 
                 * TODO There should be a means to specify a filter on the
                 * possible predicates to be used for traversal. If there is a
                 * single predicate, then that gives us S+P bound. If there are
                 * multiple predicates, then we have an IElementFilter on P (in
                 * addition to the filter that is removing the Literals from the
                 * scan).
                 * 
                 * TODO Use the chunk parallelism? Explicit for(x : chunk)?
                 */
                final IChunkedOrderedIterator<ISPO> eitr = kb
                        .getSPORelation()
                        .getAccessPath(v/* s */, null/* p */, null/* o */,
                                null/* c */, filter).iterator();
                try {
                    while (eitr.hasNext()) {

                        final ISPO edge = eitr.next();

                        final IV u = edge.o();

                        if (visited.add(u)) {

                            // Add to the new frontier.
                            ftp1.add(u);

                        }
                    }
                } finally {
                    eitr.close();
                }
            }

        }

    }

    /**
     * Return a random vertex.
     * 
     * @param kb
     * @return
     */
    private static IV getRandomVertex(final AbstractTripleStore kb) {
        
        // Select a random starting vertex.
        IV startingVertex = null;
        {
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

            // Truncate at MAX_INT.
            final int size = (int) Math.min(ndx.rangeCount(),
                    Integer.MAX_VALUE);

            while (size > 0L && startingVertex == null) {

                final int rindex = new Random().nextInt(size);

                /*
                 * Use tuple that will return both the key and the value so
                 * we can decode the entire tuple.
                 */
                final Tuple<ISPO> tuple = new Tuple<ISPO>(ndx,
                        IRangeQuery.KEYS | IRangeQuery.VALS);

                if (ndx.valueAt(rindex, tuple) == null) {

                    /*
                     * This is a deleted tuple. Try again.
                     * 
                     * Note: This retry is NOT safe for production use. The
                     * index might not have any undeleted tuples. However,
                     * we should not be using an index without any undeleted
                     * tuples for a test harness, so this should be safe
                     * enough here. If you want to use this production, at a
                     * mimimum make sure that you limit the #of attempts for
                     * the outer loop.
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
            throw new RuntimeException("No starting vertex");

        return startingVertex;

    }
    
    /**
     * Test routine to running against a {@link Journal} in which some data set
     * has already been loaded.
     * 
     * @param args
     * @throws Exception 
     */
    public static void main(final String[] args) throws Exception {

        // The property file (for an existing Journal).
        final File propertyFile = new File("RWStore.properties");

        // The namespace of the KB.
        final String namespace = "kb";

        final boolean quiet = false;

        final Properties properties = new Properties();
        {
            if (!quiet)
                System.out.println("Reading properties: " + propertyFile);
            final InputStream is = new FileInputStream(propertyFile);
            try {
                properties.load(is);
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }

        final Journal jnl = new Journal(properties);

        try {

            final long timestamp = jnl.getLastCommitTime();

            final AbstractTripleStore kb = (AbstractTripleStore) jnl
                    .getResourceLocator().locate(namespace, timestamp);

            if (kb == null)
                throw new RuntimeException("No such KB: " + namespace);

            final Random r = new Random(217L);
            
            final IV[] samples = BigdataGASUtil
                    .getRandomSample(r, kb, 100/* desiredSampleSize */);

            for (int i = 0; i < samples.length; i++) {

                final IV startingVertex = samples[i];

                final BFS h = new BFS(kb, startingVertex);

                // h.init();

                h.run();

            }
            
        } finally {

            jnl.close();

        }

    }

}
