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

/**
 * Utility class declaring methods for examining a stack trace for an instance
 * of some class of exception.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class InnerCause {

    /**
     * Examines a stack trace for an instance of the specified cause nested to
     * any level within that stack trace.
     * 
     * @param t
     *            The stack trace.
     * @param cls
     *            The class of exception that you are looking for in the stack
     *            trace.
     *            
     * @return An exception that is an instance of that class iff one exists in
     *         the stack trace and <code>null</code> otherwise.
     *         
     * @throws IllegalArgumentException
     *             if any parameter is null.
     */
//    static public Throwable getInnerCause(Throwable t, Class cls) {
    static public Throwable getInnerCause(Throwable t, Class<? extends Throwable> cls) {
        
        if (t == null)
            throw new IllegalArgumentException();

        if (cls == null)
            throw new IllegalArgumentException();
        
        {
            Class x = t.getClass();
            while(x != null){
                if( x == cls) 
                    return t;
                x = x.getSuperclass();
            }
            
        }
         
        t = t.getCause();

        if (t == null)
            return null;

        return getInnerCause(t, cls);

    }

    /**
     * Examines a stack trace for an instance of the specified cause nested to
     * any level within that stack trace.
     * 
     * @param t
     *            The stack trace.
     * @param cls
     *            The class of exception that you are looking for in the stack
     *            trace.
     * 
     * @return <code>true</code> iff an exception that is an instance of that
     *         class iff one exists in the stack trace.
     * 
     * @throws IllegalArgumentException
     *             if any parameter is null.
     */
//    static public boolean isInnerCause(Throwable t, Class cls) {
        // Note: Use of generics commented out for 1.4 compatibility.
    static public boolean isInnerCause(Throwable t, Class<? extends Throwable>cls) {
        
        return getInnerCause(t, cls) != null;
        
    }
    
}
