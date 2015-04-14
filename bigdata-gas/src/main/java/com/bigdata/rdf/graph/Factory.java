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
package com.bigdata.rdf.graph;

/**
 * Singleton pattern for initializing a vertex state or edge state object
 * given the vertex or edge.
 * 
 * @param <V>
 *            The vertex or the edge.
 * @param <T>
 *            The object that will model the state of that vertex or edge in
 *            the computation.
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public abstract class Factory<V, T> {

    /**
     * Factory pattern.
     * 
     * @param value
     *            The value that provides the scope for the object.
     *            
     * @return The factory generated object.
     */
    abstract public T initialValue(V value);
    
}