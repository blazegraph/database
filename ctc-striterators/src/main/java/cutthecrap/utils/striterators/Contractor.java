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
 * Used with a Contractorator to contract an Expanded iterator
 * 
 * The Contractorator will call contract on this object
 * 
 * @author Martyn Cutcher
 * 
 * @todo CONTRACTOR annotation and allow it to be set as an argument, moving
 *       contract() onto the contractorator.
 * 
 * @todo CONTRACTOR should be able to break an iterator into many chunks, not
 *       just one.  Maybe the API should return an Iterator from an Iterator
 *       in which the chunkiness is changed?
 */
public abstract class Contractor extends FilterBase {

	protected Object m_state = null;

	public Contractor()	{	}

	public Contractor(Object state) {
		m_state = state;
	}
	
	//-------------------------------------------------------------

	@Override
    public Iterator filterOnce(Iterator src, Object context) {
        return new Contractorator(src, context, this);
	}

	//-------------------------------------------------------------

	protected abstract Object contract(Iterator src);
}
