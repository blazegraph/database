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
 * Created on Jan 24, 2008
 */

package com.bigdata.btree.proc;

import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.service.Split;

/**
 * Aggregates the value of an {@link Long} result.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LongAggregator implements IResultHandler<Long, Long> {

    private final AtomicLong counter = new AtomicLong(0);

    public LongAggregator() {
        
    }
    
    /**
     * 
     * @todo watch for overflow of {@link Long#MAX_VALUE}
     */
    public void aggregate(Long result, Split split) {

        counter.addAndGet(result.longValue());

    }

    public Long getResult() {

        return counter.get();

    }

}
