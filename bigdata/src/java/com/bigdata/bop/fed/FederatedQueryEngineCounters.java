/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

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
    final CAT chunksIn = new CAT();

    /**
     * The #of chunks of solutions sent.
     */
    final CAT chunksOut = new CAT();

    /**
     * The #of solutions received.
     */
    final CAT solutionsIn = new CAT();

    /**
     * The #of solutions sent.
     */
    final CAT solutionsOut = new CAT();

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

        return root;

    }

}