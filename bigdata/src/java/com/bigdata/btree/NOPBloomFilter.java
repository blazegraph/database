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
 * Created on Nov 6, 2008
 */

package com.bigdata.btree;

/**
 * A bloom filter that never reports <code>false</code> (this means that you
 * must always check the index) and that does not permit anything to be added
 * and, in fact, has no state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class NOPBloomFilter implements IBloomFilter {

    public static final transient NOPBloomFilter INSTANCE = new NOPBloomFilter();
    
    /**
     * @throws UnsupportedOperationException
     *             always.
     */
    public boolean add(byte[] key) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Returns <code>true</code>.
     * <p>
     * Note: The NOP filter returns <code>true</code> because a
     * <code>true</code> return by a bloom filter means that you have to test
     * the data, which is how we achieve NOP semantics. If this method were to
     * return <code>false</code> then it would be claiming that the key was
     * not in the index!!!
     * 
     * @return <code>true</code>.
     */
    public boolean contains(byte[] key) {
        
        return true;
        
    }

    /**
     * Returns <code>true</code>.
     */
    public boolean isEnabled() {
        
        return true;
        
    }

    public void falsePos() {
     
        // NOP
        
    }
    
}
