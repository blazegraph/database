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
package com.bigdata.service.mapReduce.tasks;

import java.util.Iterator;
import java.util.UUID;

import com.bigdata.service.mapReduce.AbstractReduceTask;

/**
 * Summarizes tuples of the form <code>{key, term}</code>.
 * <p>
 * Note that many terms may be conflated into the same Unicode sort key
 * depending on the collator that you are using. This task just deserializes
 * the 1st term entry for each distinct key. If you want some consistency in
 * the reported terms, then you should normalize the terms in your map task.
 * 
 * @see ExtractKeywords
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CountKeywords extends AbstractReduceTask {

    /**
     * 
     */
    private static final long serialVersionUID = -4264802500123849163L;

    public CountKeywords(UUID uuid, UUID dataService) {
        
        super(uuid, dataService );
        
    }
    
    public void reduce(byte[] key, Iterator<byte[]> vals) throws Exception {

        String term = null;

        boolean first = true;

        long count = 0L;

        while (vals.hasNext()) {

            byte[] val = vals.next();

            if (first) {

                term = new String(val, ExtractKeywords.UTF8);

                first = false;

            }
            
            count++;

        }

        if (count == 0)
            throw new AssertionError();
        
        System.err.println(term+" : "+count);
        
    }

}