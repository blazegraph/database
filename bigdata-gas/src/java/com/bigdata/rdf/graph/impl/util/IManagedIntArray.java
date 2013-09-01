/**
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.rdf.graph.impl.util;

/**
 * An interface for a managed int[]. Implementations of this interface may
 * permit transparent extension of the managed int[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: IManagedByteArray.java 4548 2011-05-25 19:36:34Z thompsonbry $
 */
public interface IManagedIntArray extends IIntArraySlice {

    /**
     * Return the capacity of the backing buffer.
     */
    int capacity();

    /**
     * Ensure that the buffer capacity is a least <i>capacity</i> total values.
     * The buffer may be grown by this operation but it will not be truncated.
     * 
     * @param capacity
     *            The minimum #of values in the buffer.
     */
    void ensureCapacity(int capacity);
    
}
