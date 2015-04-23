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
package cutthecrap.utils.striterators;

import java.util.Iterator;

/**
 * Expander
 *
 * Used with Expanderator by Striterator to expand returned objects.
 *
 * Derivations must implement expand method.
 * 
 * If an Expander wants to track the context it should return a non-null
 * value for getContextMgr.  This ensures that the Expanderator will not
 * optimize itself away in a tail recursion operation that would prevent
 * the correct protocol implementation.
 */

public abstract class Expander extends FilterBase implements IContextMgr {

	public Expander()	{	}

	//-------------------------------------------------------------

    @Override
    final public Iterator filterOnce(Iterator src, Object context) {
        return new Expanderator(src, context, this);
    }

    // -------------------------------------------------------------

    protected abstract Iterator expand(Object obj);

    /**
     * callback to implementation when expansion is complete
     */
	public void popContext() {
		// NOP
	}
	
	public void pushContext(Object context) {
		// NOP
	}
	
	protected IContextMgr getContextMgr() {
		return null;
	}
}