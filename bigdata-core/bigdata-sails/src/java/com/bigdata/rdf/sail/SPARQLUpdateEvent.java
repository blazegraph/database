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
package com.bigdata.rdf.sail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rdf.sparql.ast.Update;

/**
 * An event reflecting progress for some sequence of SPARQL UPDATE operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SPARQLUpdateEvent {

    private final Update op;
    private final long elapsed;
    /** The time to flush the sail connection before executing the next update operation in an update request. */
    private final long connectionFlushNanos;
    /** The time to batch resolve any previously unknown RDF Values to their IVs before executing the next update operation. */
    private final long batchResolveNanos;
    private DeleteInsertWhereStats deleteInsertWhereStats;
    private final Throwable cause;

    /**
	 * Class reports back the time for the WHERE clause, DELETE clause (if any),
	 * and INSERT clause (if any) for a DELETE/INSERT WHERE operation.
	 *
	 * @see BLZG-1446 (Provide detailed statistics on execution performance
	 *      inside of SPARQL UPDATE requests).
	 */
    public static class DeleteInsertWhereStats {
    	final public AtomicLong whereNanos = new AtomicLong();
    	final public AtomicLong deleteNanos = new AtomicLong();
    	final public AtomicLong insertNanos = new AtomicLong();
    }
    
    /**
	 * 
	 * @param op
	 *            The {@link Update} operation.
	 * @param elapsed
	 *            The elapsed time (nanoseconds).
	 * @param cause
	 *            The cause iff an error occurred and otherwise
	 *            <code>null</code>.
	 * @param deleteInsertWhereStats
	 *            Statistics associated with the processing of a DELETE/INSERT
	 *            WHERE operation (and otherwise <code>null</code>).
	 */
	public SPARQLUpdateEvent(final Update op, final long elapsed, final long connectionFlushNanos, final long batchResolveNanos, final Throwable cause,
			final DeleteInsertWhereStats deleteInsertWhereStats) {

        if (op == null)
            throw new IllegalArgumentException();
        
        this.op = op;
        
        this.elapsed = elapsed;

        this.connectionFlushNanos = connectionFlushNanos;
        
		this.batchResolveNanos = batchResolveNanos;

        this.deleteInsertWhereStats = deleteInsertWhereStats;
        
        this.cause = cause;
        
    }
    
    /**
     * Return the update operation.
     */
    public Update getUpdate() {
        return op;
    }

    /**
     * Return the elapsed runtime for that update operation in nanoseconds.
     */
    public long getElapsedNanos() {
        return elapsed;
    }

    /**
	 * Return the time required to flush the sail connection before executing
	 * the corresponding SPARQL UPDATE operation within a SPARQL UPDATE request
	 * that has multiple operations (the sail connection is flushed before each
	 * operation except the first).
	 * 
	 * @see BLZG-1306
	 */
    public long getConnectionFlushNanos() {
    	return connectionFlushNanos;
    }
    
    /**
	 * Return the time required to batch resolve any unknown RDF Values against
	 * the dictionary indices before executing the corresponding SPARQL UPDATE
	 * operation within a SPARQL UPDATE request that has multiple operations
	 * (batch resolution must be performed before each UPDATE operation in case
	 * an RDF Value not known at the time that the SPARQL UPDATE parser was run
	 * has since become defined through a side-effect of a previous SPARQL UPDATE
	 * request within the scope of the same connection).
	 * 
	 * @see BLZG-1306
	 */
    public long getBatchResolveNanos() {
    	return batchResolveNanos;
    }
    
    /**
     * The cause iff an error occurred and otherwise <code>null</code>.
     */
    public Throwable getCause() {
        return cause;
    }
    
    /**
     * Return statistics associated with the processing of a DELETE/INSERT
	 *            WHERE operation (and otherwise <code>null</code>).
	 *            
	 * @see BLZG-1446 (Provide detailed statistics on execution performance
	 *      inside of SPARQL UPDATE requests).
     */
    public DeleteInsertWhereStats getDeleteInsertWhereStats() {
        return deleteInsertWhereStats;
    }

    /**
     * Incremental progress report during <code>LOAD</code>.
     */
    public static class LoadProgress extends SPARQLUpdateEvent {

        private final long nparsed;
        private final boolean done;

        public LoadProgress(final Update op, final long elapsed,
                final long nparsed, final boolean done) {

			super(op, elapsed, 0L /* connectionFlushNanos */, 0L /* batchResolveNanos */, null/* cause */,
					null/* deleteInsertWhereStats */);

            this.nparsed = nparsed;
            
            this.done = done;

        }

        /**
         * Return the #of statements parsed as of the moment that this event was
         * generated.
         * <P>
         * Note: Statements are incrementally written onto the backing store.
         * Thus, the parser will often run ahead of the actual index writes.
         * This can manifest as periods during which the "parsed" statement
         * count climbs quickly followed by periods in which it is stable. That
         * occurs when the parser is blocked because it can not add statements
         * to the connection while the connection is being flushed to the
         * database. There is a ticket to fix this blocking behavior so the
         * parser can continue to run while we are flushing statements onto the
         * database - see below.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/529">
         *      Improve load performance </a>
         */
        public long getParsedCount() {

            return nparsed;

        }
        
        /**
         * Return <code>true</code> iff the LOAD operation has finished parsing
         * the document.
         * <p>
         * Note: This does not mean that the statements have been written
         * through to the disk, just that the parsed is done running.
         */
        public boolean isDone() {
            
            return done;
            
        }
        
        /**
         * Report the parser rate in triples per second.
         */
        public long triplesPerSecond() {

            long elapsedMillis = TimeUnit.NANOSECONDS
                    .toMillis(getElapsedNanos());

            if (elapsedMillis == 0) {

                // Note: Avoid divide by zero error.
                elapsedMillis = 1;

            }

            return ((long) (((double) nparsed) / ((double) elapsedMillis) * 1000d));

        }

    }

}
