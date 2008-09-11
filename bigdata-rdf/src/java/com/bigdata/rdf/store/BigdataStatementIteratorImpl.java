/*

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
package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Wraps the raw iterator that traverses a statement index and exposes each
 * visited statement as a {@link BigdataStatement} (batch API).
 * 
 * @todo The resolution of term identifiers to terms should happen during
 *       asynchronous read-ahead for even better performance (less latency).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataSolutionResolverator
 */
public class BigdataStatementIteratorImpl implements BigdataStatementIterator {

    final protected static Logger log = Logger.getLogger(BigdataStatementIteratorImpl.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();
    
    /**
     * The database whose lexicon will be used to resolve term identifiers to
     * terms.
     */
    private final AbstractTripleStore db;
    
    /**
     * The index of the last entry returned in the current {@link #chunk} and
     * <code>-1</code> until the first entry is returned.
     */
    private int lastIndex = -1;

    /**
     * The current chunk of resolved {@link BigdataStatement}s.
     */
    private BigdataStatement[] chunk = null;

    /**
     * Factory for creating {@link BigdataValue}s and {@link BigdataStatement}s
     * from the {@link ISPO}s visited by the {@link #src} iterator.
     */
    final private BigdataValueFactory valueFactory;

    /**
     * Total elapsed time for the iterator instance.
     */
    private long elapsed = 0L;
    
    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     */
    public BigdataStatementIteratorImpl(AbstractTripleStore db,
            IChunkedOrderedIterator<ISPO> src) {

        if (db == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.db = db;
        
        this.valueFactory = db.getValueFactory();

        /*
         * The queue capacity controls how many chunks of SPO[]s can be resolved
         * to BigdataStatements in advance of visitation by the [resolvedItr].
         * You can use a SynchronousQueue for NO buffering or a queue with
         * either a fixed or variable capacity.
         */
//        final BlockingQueue<BigdataStatement[]> queue = new SynchronousQueue<BigdataStatement[]>();
//        final BlockingQueue<BigdataStatement[]> queue = new ArrayBlockingQueue<BigdataStatement[]>(10);

        /*
         * Create a buffer for chunks of resolved BigdataStatements.
         * 
         * FIXME Configuration for [chunkOfChunksCapacity] but also test with
         * a SynchronousQueue.
         */
        final int chunkOfChunksCapacity = 100;
        buffer = new BlockingBuffer<BigdataStatement[]>(chunkOfChunksCapacity);

        /*
         * Create and run a task which reads ISPO chunks from the source
         * iterator and writes resolved BigdataStatement chunks on the buffer.
         */
        final Future f = db.getIndexManager().getExecutorService().submit(
                new ChunkConsumer(src));

        /*
         * Set the future for that task on the buffer.
         */
        buffer.setFuture(f);
        
        /*
         * This class will read resolved chunks from the [resolvedItr] and then
         * hand out BigdataStatements from the current [chunk].
         */
        resolvedItr = buffer.iterator();
        
    }

    /**
     * Buffer of {@link BigdataStatement}[] chunks resolved from {@link ISPO}s[]
     * chunks.
     */
    private final BlockingBuffer<BigdataStatement[]> buffer;
    
    /**
     * Iterator draining resolved {@link BigdataStatement}[] chunks from the
     * {@link #buffer}.
     */
    private final Iterator<BigdataStatement[]> resolvedItr;

//    /**
//     * Queue of converted chunks.
//     */
//    private SynchronousQueue<BigdataStatement[]> queue = new SynchronousQueue<BigdataStatement[]>();

    /**
     * Consumes chunks from the source iterator, placing the converted chunks on
     * a queue.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private class ChunkConsumer implements Runnable {

        /**
         * The source iterator (will be closed when this iterator is closed).
         */
        private final IChunkedOrderedIterator<ISPO> src;
        
        public ChunkConsumer(IChunkedOrderedIterator<ISPO> src) {

            if (src == null)
                throw new IllegalArgumentException();
            
            this.src = src;
            
        }
        
        public void run() {

            try {

                if (INFO)
                    log.info("Start");

                while (src.hasNext()) {

                    buffer.add(resolveNextChunk());
                        
                }

                if (INFO)
                    log.info("Finished");

            } finally {

                src.close();
                
                buffer.close();

            }

        }
        
        /**
         * Fetches the next chunk from the source iterator and resolves the term
         * identifiers, producing a chunk of resolved {@link BigdataStatement}s.
         */
        protected BigdataStatement[] resolveNextChunk() {
            
            if (INFO)
                log.info("Fetching next chunk");
            
            // fetch the next chunk of SPOs.
            final ISPO[] chunk = src.nextChunk();

            if (INFO)
                log.info("Fetched chunk: size=" + chunk.length);

            /*
             * Create a collection of the distinct term identifiers used in this
             * chunk.
             */

//            LongOpenHashSet ids = new LongOpenHashSet(chunk.length*4);
            
            final Collection<Long> ids = new HashSet<Long>(chunk.length * 4);

            for (ISPO spo : chunk) {

                ids.add(spo.s());

                ids.add(spo.p());

                ids.add(spo.o());

                if (spo.hasStatementIdentifier()) {

                    ids.add(spo.getStatementIdentifier());

                }

            }

            if (INFO)
                log.info("Resolving " + ids.size() + " term identifiers");
            
            /*
             * Batch resolve term identifiers to BigdataValues, obtaining the
             * map that will be used to resolve term identifiers to terms for
             * this chunk.
             */
            final Map<Long, BigdataValue> terms = db.getLexiconRelation().getTerms(ids);

            /*
             * The chunk of resolved statements.
             */
            final BigdataStatement[] stmts = new BigdataStatement[chunk.length];
            
            int i = 0;
            for (ISPO spo : chunk) {

                /*
                 * Resolve term identifiers to terms using the map populated
                 * when we fetched the current chunk.
                 */
                final BigdataResource s = (BigdataResource) terms.get(spo.s());
                final BigdataURI p = (BigdataURI) terms.get(spo.p());
                final BigdataValue o = terms.get(spo.o());
                final BigdataResource c = (spo.hasStatementIdentifier() ? (BigdataResource) terms
                        .get(spo.getStatementIdentifier())
                        : null);

                if (spo.hasStatementType() == false) {

                    log.error("statement with no type: "
                            + valueFactory.createStatement(s, p, o, c, null));

                }

                // the statement.
                final BigdataStatement stmt = valueFactory.createStatement(s,
                        p, o, c, spo.getStatementType());

                // save the reference.
                stmts[i++] = stmt;

            }

            return stmts;

        }

    }
    
    public boolean hasNext() {

        if (lastIndex != -1 && lastIndex + 1 < chunk.length) {

            return true;
            
        }
        
        if(DEBUG) {
            
            log.debug("Testing resolved iterator.");
            
        }
        
        return resolvedItr.hasNext();
        
    }

    public BigdataStatement next() {

        final long begin = System.currentTimeMillis();
        
        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || lastIndex + 1 == chunk.length) {

            // get the next chunk of resolved BigdataStatements.
            chunk = resolvedItr.next();
            
            // reset the index.
            lastIndex = -1;

            if (INFO)
                log.info("nextChunk ready: time="
                        + (System.currentTimeMillis() - begin) + "ms");

        }

        // the next resolved statement.
        final BigdataStatement stmt = chunk[++lastIndex];
            
        if (DEBUG)
            log.debug("lastIndex=" + lastIndex + ", chunk.length="
                    + chunk.length + ", stmt=" + stmt);

        elapsed += (System.currentTimeMillis() - begin);

        return stmt;

    }

    /**
     * @throws UnsupportedOperationException
     * 
     * @todo this could be implemented if we save a reference to the last
     *       {@link SPO} visited.
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (INFO)
            log.info("elapsed=" + elapsed);

        // Note: closed by the Consumer.
//        src.close();

        chunk = null;
        
    }

}
