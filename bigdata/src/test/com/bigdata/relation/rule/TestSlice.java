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
 * Created on Sep 26, 2008
 */

package com.bigdata.relation.rule;


import junit.framework.TestCase2;

/**
 * Unit tests for {@link Slice}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSlice extends TestCase2 {

    /**
     * 
     */
    public TestSlice() {
    }

    /**
     * @param arg0
     */
    public TestSlice(String arg0) {
        super(arg0);
    }

    /**
     * Tests some fence posts for the {@link Slice} ctor, verifies the
     * computation of {@link ISlice#getLast()}, and the reporting of the
     * properties specified to the ctor.
     */
    public void test_slice_ctor() {
        
        {
            final ISlice slice = new Slice(0L, 1L);
            
            assertEquals(slice.getOffset(), 0L);
            
            assertEquals(slice.getLimit(), 1L);
            
            assertEquals(slice.getLast(), 1L);
            
        }
        
        {
            final ISlice slice = new Slice(1L, 1L);
            
            assertEquals(slice.getOffset(), 1L);
            
            assertEquals(slice.getLimit(), 1L);
            
            assertEquals(slice.getLast(), 2L);
            
        }
        
        {
            
            final ISlice slice = new Slice(0L, Long.MAX_VALUE);
            
            assertEquals(slice.getOffset(), 0L);
            
            assertEquals(slice.getLimit(), Long.MAX_VALUE);
            
            assertEquals(slice.getLast(), Long.MAX_VALUE);
            
        }

        // verify that last does not overflow.
        {
            
            final ISlice slice = new Slice(2L, Long.MAX_VALUE);
            
            assertEquals(slice.getOffset(), 2L);
            
            assertEquals(slice.getLimit(), Long.MAX_VALUE);
            
            assertEquals(slice.getLast(), Long.MAX_VALUE);
            
        }
        
    }
    
    public void test_slice_ctor_correctRejection() {

        try {

            new Slice(-1L, 100L);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            log.info("ignoring expected exception");
            
        }

        try {

            new Slice(12L, 0L);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            log.info("ignoring expected exception");
            
        }

        try {

            new Slice(1L, -1L);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            log.info("ignoring expected exception");
            
        }

    }

}
