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
 * Created on Aug 7, 2008
 */

package com.bigdata.striterator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.striterator.ChunkedStriterator;
import com.bigdata.striterator.IChunkedStriterator;
import com.bigdata.striterator.Resolver;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link Resolver}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestResolver extends TestCase2 {

    /**
     * 
     */
    public TestResolver() {

    }

    /**
     * @param arg0
     */
    public TestResolver(String arg0) {
        
        super(arg0);
        
    }

    public void test_filter() {
        
        final Map<Long,String> map = new HashMap<Long,String>();
        
        map.put(1L, "A");
        map.put(2L, "B");
        map.put(3L, "C");
        
        IChunkedStriterator itr = new ChunkedStriterator(Arrays.asList(
                new Long[] { 1L, 2L, 3L }).iterator());
        
        itr = itr.addFilter(new Resolver<Iterator<Long>, Long, String>(){

            @Override
            protected String resolve(Long e) {
                return map.get(e);
            }});
        
        assertEquals(new String[] { "A", "B", "C" }, itr.nextChunk());
        
    }
    
}
