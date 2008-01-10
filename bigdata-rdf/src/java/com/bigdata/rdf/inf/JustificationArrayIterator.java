/**

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
 * Created on Nov 14, 2007
 */

package com.bigdata.rdf.inf;

import java.util.NoSuchElementException;

/**
 * Visits {@link Justification}s from a backing array.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JustificationArrayIterator implements IJustificationIterator {

    private final Justification[] a;
    private final int len;
    
    private boolean open = true;
    private int i = 0;
    
    /**
     * 
     */
    public JustificationArrayIterator(Justification[] a, int len) {
        
        this.a = a;
        
        this.len = len;
        
    }

    public void close() {
    
        if(!open) return;
        
        open = false;
        
    }

    public Justification next() {

        if(!hasNext())
            throw new NoSuchElementException(); 
        
        Justification j = a[i++];
        
        return j;
        
    }

    /**
     * Returns the remaining {@link Justification}s.
     * 
     * @throws NoSuchElementException
     *             if {@link #hasNext()} returns false.
     */
    public Justification[] nextChunk() {
    
        final Justification[] ret;
        
        if (i == 0 && len == a.length) {
            
            /*
             * The SPO[] does not have any unused elements and nothing has been
             * returned to the caller by next() so we can just return the
             * backing array in this case.
             */
            
            ret = a;
            
        } else {

            /*
             * Create and return a new SPO[] containing only the statements
             * remaining in the iterator.
             */
            
            final int remaining = len - i;
            
            ret = new Justification[remaining];
            
            System.arraycopy(a, i, ret, 0, remaining);
            
        }
        
        // indicate that all statements have been consumed.
        
        i = len;
        
        return ret;

    }

    public boolean hasNext() {
        
        return open && i < len;

    }

    public void remove() {
   
        throw new UnsupportedOperationException();
        
    }

}
