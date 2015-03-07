/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import com.bigdata.rdf.sparql.ast.Update;

/**
 * An event reflecting progress for some sequence of SPARQL UPDATE operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SPARQLUpdateEvent {

    private final Update op;
    private final long elapsed;
    private final Throwable cause;
    
    /**
     * 
     * @param op
     *            The {@link Update} operation.
     * @param elapsed
     *            The elapsed time.
     * @param cause
     *            The cause iff an error occurred and otherwise
     *            <code>null</code>.
     */
    public SPARQLUpdateEvent(final Update op, final long elapsed,
            final Throwable cause) {

        if (op == null)
            throw new IllegalArgumentException();
        
        this.op = op;
        
        this.elapsed = elapsed;
        
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
     * The cause iff an error occurred and otherwise <code>null</code>.
     */
    public Throwable getCause() {
        return cause;
    }
    
    /**
     * Incremental progress report during <code>LOAD</code>.
     */
    public static class LoadProgress extends SPARQLUpdateEvent {

        private final long nparsed;
        private final boolean done;

        public LoadProgress(final Update op, final long elapsed,
                final long nparsed, final boolean done) {

            super(op, elapsed, null/* cause */);

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
