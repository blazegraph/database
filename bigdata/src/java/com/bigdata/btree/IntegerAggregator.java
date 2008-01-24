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
/*
 * Created on Jan 16, 2008
 */
package com.bigdata.btree;

import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.service.Split;

/**
 * Aggregates the value of an {@link Integer} result, making the sum available
 * as a {@link Long} integer (to help prevent overflow).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IntegerAggregator implements IResultHandler<Integer, Long> {

    private final AtomicLong counter = new AtomicLong(0);

    public IntegerAggregator() {
        
    }
    
    public void aggregate(Integer result, Split split) {

        counter.addAndGet(result.intValue());

    }

    public Long getResult() {

        return counter.get();

    }

}