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
 * Type-safe enumeration characterizing the assumptions of an algorithm
 * concerning its initial frontier.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public enum FrontierEnum {

    /**
     * The initial frontier is a single vertex.
     */
    SingleVertex,
    
    /**
     * The initial frontier is all vertices.
     */
    AllVertices,

    /**
     * The initial frontier is an unweighted sample over all of the vertices
     */
    SampledVertices;
    
}
