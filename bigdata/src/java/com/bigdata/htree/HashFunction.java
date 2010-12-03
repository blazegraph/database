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
 * Created on Dec 1, 2010
 */
package com.bigdata.htree;

import com.bigdata.btree.BloomFilter;

/**
 * A hashing function.
 * 
 * @todo Multi-dimensional order preserving hashing using an order key[] and has
 *       a fixed schema for the key components.
 * 
 * @todo Consider the {@link BloomFilter} hash code logic. It is based on a
 *       table of functions. It might be a good fit here.
 * 
 * @todo Is is worth while to keep the key and the hash code together in a small
 *       structure? It is inexpensive to mask off the bits that we do not want
 *       to consider, but we need to avoid passing the masked off hash code into
 *       routines which expect the full hash code.
 * 
 * @todo We could also use cryptographic hash functions to have a better
 *       guarantee of a uniform hash value distribution. To do that we would
 *       have to generalize the size of the hash value from int32, perhaps using
 *       a generic type or a BitVector.
 * 
 * @todo The hash function should be a configuration option store with the index
 *       metadata.
 */
public interface HashFunction {

	public int hash(Object key);
	
}
