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
 * Extended {@link IGASScheduler} interface. This interface is exposed to the
 * implementation of the GAS Engine. The methods on this interface are NOT for
 * use by the {@link IGASProgram} and MIGHT NOT (really, should not) be
 * available on the {@link IGASScheduler} supplied to an {@link IGASProgram}.
 * 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface IGASSchedulerImpl extends IGASScheduler {

    /**
     * Compact the schedule into the new frontier.
     */
    void compactFrontier(IStaticFrontier frontier);

    /**
     * Reset all internal state (and get rid of any thread locals).
     */
    void clear();

}
