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
 * Created on Feb 27, 2008
 */

package com.bigdata.util;

import java.io.IOException;

import junit.framework.TestCase;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestInnerCause extends TestCase {

    /**
     * 
     */
    public TestInnerCause() {
        super();
    }

    /**
     * @param arg0
     */
    public TestInnerCause(String arg0) {
        super(arg0);
    }

    protected Throwable getInnerCause(Throwable t, Class<? extends Throwable> cls) {
        
        return InnerCause.getInnerCause(t, cls);
        
    }
    
    public void test_getInnerCause_correctRejection() {

        try {
            getInnerCause(null, null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            getInnerCause(null, RuntimeException.class);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try {
            getInnerCause(new RuntimeException(), null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

    }
    
    /**
     * Finds cause when it is on top of the stack trace and the right type.
     */
    public void test_getInnerCause01_find_exact() {

        Throwable t = new RuntimeException();
         
        assertTrue(t == getInnerCause(t, RuntimeException.class));
            
    }

    /**
     * Find cause when it is on top of the stack trace and a subclass of the
     * desired type.
     */
    public void test_getInnerCause01_find_subclass() {

        Throwable t = new IOException();
         
        assertTrue(t == getInnerCause(t, Exception.class));
        
    }

    /**
     * Does not find cause that is a super class of the desired type.
     */
    public void test_getInnerCause01_reject_superclass() {

        Throwable t = new Exception();
         
        assertNull(getInnerCause(t, IOException.class));
        
    }
    
    /**
     * Does not find cause when it is on top of the stack trace and not either
     * the desired type or a subclass of the desired type.
     */
    public void test_getInnerCause01_reject_otherType() {

        Throwable t = new Throwable();
         
        assertNull(getInnerCause(t, Exception.class));
        
    }

    /**
     * Finds inner cause that is the exact type.
     */
    public void test_getInnerCause02_find_exact() {

        Throwable cause = new Exception();

        Throwable t = new Throwable(cause);

        assertTrue(cause == getInnerCause(t, Exception.class));

    }

    /**
     * Finds inner cause that is a derived type (subclass).
     */
    public void test_getInnerCause02_find_subclass() {

        Throwable cause = new IOException();

        Throwable t = new Throwable(cause);

        assertTrue(cause == getInnerCause(t, Exception.class));

    }

    /**
     * Does not find inner cause that is a super class of the desired type.
     */
    public void test_getInnerCause02_reject_superclass() {

        Throwable cause = new Exception();
        
        Throwable t = new RuntimeException(cause);
         
        assertNull( getInnerCause(t, IOException.class));
        
    }
    
    /**
     * Does not find an inner cause that is neither the specified type nor a
     * subtype of the specified type.
     */
    public void test_getInnerCause03_reject_otherType() {

        Throwable cause = new RuntimeException();

        Throwable t = new Exception(cause);

        assertNull( getInnerCause(t, IOException.class) );

    }
    
}
