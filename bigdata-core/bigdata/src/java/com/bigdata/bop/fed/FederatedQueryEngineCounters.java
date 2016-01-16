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
 * Created on Feb 8, 2012
 */

package com.bigdata.bop.fed;

import com.bigdata.bop.engine.QueryEngineCounters;
import com.bigdata.counters.CAT;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;

/**
 * Extended performance counters for the {@link FederatedQueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FederatedQueryEngineCounters extends QueryEngineCounters {

    /**
     * The #of chunks of solutions received.
     */
    protected final CAT chunksIn = new CAT();

    /**
     * The #of chunks of solutions sent.
     */
    protected final CAT chunksOut = new CAT();

    /**
     * The #of solutions received.
     */
    protected final CAT solutionsIn = new CAT();

    /**
     * The #of solutions sent.
     */
    protected final CAT solutionsOut = new CAT();

    @Override
    public CounterSet getCounters() {

        final CounterSet root = super.getCounters();

        // #of chunks in.
        root.addCounter("chunksIn", new Instrument<Long>() {
            public void sample() {
                setValue(chunksIn.get());
            }
        });

        // #of chunks out.
        root.addCounter("chunksOut", new Instrument<Long>() {
            public void sample() {
                setValue(chunksOut.get());
            }
        });

        // #of chunks in.
        root.addCounter("solutionsIn", new Instrument<Long>() {
            public void sample() {
                setValue(solutionsIn.get());
            }
        });

        // #of chunks out.
        root.addCounter("solutionsOut", new Instrument<Long>() {
            public void sample() {
                setValue(solutionsOut.get());
            }
        });

        // average #of chunks flowing out of the query controller per query
        root.addCounter("chunksOutPerQuery", new Instrument<Double>() {
            public void sample() {
                final long nchunks = chunksOut.get();
                final long n = queryStartCount.get();
                final double d = n == 0 ? 0d : (nchunks / (double) n);
                setValue(d);
            }
        });

        // average #of chunks flowing into the query controller per query
        root.addCounter("chunksInPerQuery", new Instrument<Double>() {
            public void sample() {
                final long nchunks = chunksIn.get();
                final long n = queryStartCount.get();
                final double d = n == 0 ? 0d : (nchunks / (double) n);
                setValue(d);
            }
        });

        // average #of solutions flowing out of the query controller per query
        root.addCounter("solutionsOutPerQuery", new Instrument<Double>() {
            public void sample() {
                final long nsol = solutionsOut.get();
                final long n = queryStartCount.get();
                final double d = n == 0 ? 0d : (nsol / (double) n);
                setValue(d);
            }
        });

        // average #of solutions flowing into the query controller per query
        root.addCounter("solutionsInPerQuery", new Instrument<Double>() {
            public void sample() {
                final long nsol = solutionsIn.get();
                final long n = queryStartCount.get();
                final double d = n == 0 ? 0d : (nsol / (double) n);
                setValue(d);
            }
        });

        // average #of solutions per chunk flowing out of the query controller
        root.addCounter("solutionsOutPerChunk", new Instrument<Double>() {
            public void sample() {
                final long nsol = solutionsOut.get();
                final long nchunks = chunksOut.get();
                final double d = nchunks == 0 ? 0d : (nsol / (double) nchunks);
                setValue(d);
            }
        });

        // average #of solutions per chunk flowing into the query controller
        root.addCounter("solutionsInPerChunk", new Instrument<Double>() {
            public void sample() {
                final long nsol = solutionsIn.get();
                final long nchunks = chunksIn.get();
                final double d = nchunks == 0 ? 0d : (nsol / (double) nchunks);
                setValue(d);
            }
        });

        return root;

    }

}
