/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Apr 6, 2009
 */

package com.bigdata.counters.query;

import java.util.Iterator;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.PeriodEnum;

/**
 * Reads counters from a {@link CounterSet}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CounterSetSelector implements ICounterSelector {

    protected static final Logger log = Logger.getLogger(CounterSetSelector.class);

    private final CounterSet counterSet;
    
    public CounterSetSelector(final CounterSet counterSet) {

        if (counterSet == null)
            throw new IllegalArgumentException();

        this.counterSet = counterSet;
        
    }

    public CounterSet getRoot() {
        
        return counterSet;
        
    }
    
    /*
     * Note: logic was modified to no longer consider the relative depth, only
     * the absolute depth.
     * 
     * FIXME does not use [fromTime, toTime, or period].
     */
    public ICounter[] selectCounters(final int depth, final Pattern pattern,
            final long fromTime, final long toTime, final PeriodEnum period) {

//        // depth of the hierarchy at the point where we are starting.
//        final int ourDepth = counterSet.getDepth();

        if (log.isInfoEnabled())
            log.info("path=" + counterSet.getPath() + ", depth=" + depth);

        final Vector<ICounter> counters = new Vector<ICounter>();
        
        final Iterator<ICounter> itr = counterSet.getCounters(pattern);

        int nscanned = 0;
        int nskipped = 0;
        
        while (itr.hasNext()) {

            final ICounter c = itr.next();

            nscanned++;
            
            if(!(c.getInstrument() instanceof HistoryInstrument)) {

                // prune non-history counters.
                if (log.isDebugEnabled())
                    log.debug("skipping: " + c.getPath());

                nskipped++;
                
                continue;
                
            }
            
            if (log.isDebugEnabled())
                log.debug("considering: " + c.getPath());
            
            if (depth != 0) {

                final int counterDepth = c.getDepth();

                if (counterDepth > depth) {

                    // prune by depth
                    if (log.isDebugEnabled())
                        log.debug("skipping: " + c.getPath());

                    nskipped++;

                    continue;
                    
                }
                
            }
            
            counters.add( c );
            
        }

        if (log.isInfoEnabled())
            log.info("Matched " + counters.size() + " counters; nscanned="
                    + nscanned + ", nskipped=" + nskipped);
        
        final ICounter[] a = counters.toArray(new ICounter[counters.size()]);

        return a;
        
    }

}
