/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.rdf.graph.IGASStats;
import com.bigdata.rdf.graph.util.GASUtil;

/**
 * Statistics for GAS algorithms.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GASStats implements IGASStats {

    private final AtomicLong nrounds = new AtomicLong();
    private final AtomicLong frontierSize = new AtomicLong();
    private final AtomicLong nedges = new AtomicLong();
    private final AtomicLong elapsedNanos = new AtomicLong();

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#add(long, long, long)
     */
    @Override
    public void add(final long frontierSize, final long nedges,
            final long elapsedNanos) {

        this.nrounds.incrementAndGet();
        
        this.frontierSize.addAndGet(frontierSize);

        this.nedges.addAndGet(nedges);

        this.elapsedNanos.addAndGet(elapsedNanos);

    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#add(com.bigdata.rdf.graph.impl.IFOO)
     */
    @Override
    public void add(final IGASStats o) {

        nrounds.addAndGet(o.getNRounds());
        
        frontierSize.addAndGet(o.getFrontierSize());
        
        nedges.addAndGet(o.getNEdges());

        elapsedNanos.addAndGet(o.getElapsedNanos());
        
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getNRounds()
     */
    @Override
    public long getNRounds() {
        return nrounds.get();
    }
    
    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getFrontierSize()
     */
    @Override
    public long getFrontierSize() {
        return frontierSize.get();
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getNEdges()
     */
    @Override
    public long getNEdges() {
        return nedges.get();
    }

    /* (non-Javadoc)
     * @see com.bigdata.rdf.graph.impl.IFOO#getElapsedNanos()
     */
    @Override
    public long getElapsedNanos() {
        return elapsedNanos.get();
    }

    /**
     * Return a useful summary of the collected statistics.
     */
    @Override
    public String toString() {

        return "nrounds=" + getNRounds()//
                + ", fontierSize=" + getFrontierSize() //
                + ", ms=" + TimeUnit.NANOSECONDS.toMillis(getElapsedNanos())//
                + ", edges=" + getNEdges()//
                + ", teps=" + GASUtil.getTEPS(getNEdges(), getElapsedNanos())//
        ;
    }
    
}
