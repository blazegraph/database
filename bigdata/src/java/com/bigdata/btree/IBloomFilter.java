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
 * Created on Nov 4, 2008
 */

package com.bigdata.btree;


/**
 * Interface for bloom filter implementations using an unsigned byte[] key.
 * <p>
 * Bloom filters give a 100% guarentee when reporting that a key was NOT found
 * but only a statistical guarentee when reporting that a key was found.
 * Therefore if either {@link #add(byte[])} or {@link #contains(byte[])} reports
 * <code>true</code> then you MUST also test the data into order to determine
 * whether the response is a <em>false positive</em>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBloomFilter {

    /**
     * Adds the key to the filter.
     * 
     * @param key
     *            The key.
     * 
     * @return <code>true</code> iff the filter state was unchanged by the
     *         incorporation of this key into the filter.
     * 
     * @throws IllegalArgumentException
     *             if <i>key</i> is <code>null</code>.
     * @throws UnsupportedOperationException
     *             if the filter does not allow mutation.
     */
    public boolean add(byte[] key);

    /**
     * Test the filter for the key <strong>a <code>true</code> return DOES NOT
     * guarentee that the key has been added to the filter while a
     * <code>false</code> return guarentees that the key HAS NOT been added to
     * the filter</strong>.
     * 
     * @param key
     *            The key.
     * 
     * @return <code>true</code> if the filter has either that key or some key
     *         that is hash equivalent to that key using the hashing function
     *         imposed by the filter; <code>false</code> iff the filter can
     *         guarentee that the key has not been added to the filter.
     * 
     * @throws IllegalArgumentException
     *             if <i>key</i> is <code>null</code>.
     */
    public boolean contains(byte[] key);

    /**
     * Notify the bloom filter that a false positive was observed for a key that
     * for which {@link #add(byte[])} reported <code>true</code> (the key was
     * in fact not in the index). This method exists solely for reporting and
     * tracking the actual error rate of the bloom filter.
     */
    public void falsePos();
    
}
