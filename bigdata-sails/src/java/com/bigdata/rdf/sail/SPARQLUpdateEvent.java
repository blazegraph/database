/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.rdf.sail;

import com.bigdata.rdf.sparql.ast.Update;

/**
 * An event reflecting progress for some sequence of SPARQL UPDATE operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Support incremental reporting on long running operations,
 *         especially LOAD.
 */
public class SPARQLUpdateEvent {

    private final Update op;
    private final long elapsed;
    
    public SPARQLUpdateEvent(final Update op, final long elapsed) {

        this.op = op;
        this.elapsed = elapsed;
        
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
     * Incremental progress report during <code>LOAD</code>.
     */
    public static class LoadProgress extends SPARQLUpdateEvent {

        private final long nparsed;

        public LoadProgress(final Update op, final long elapsed,
                final long nparsed) {

            super(op, elapsed);

            this.nparsed = nparsed;

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
        
    }
    
}
