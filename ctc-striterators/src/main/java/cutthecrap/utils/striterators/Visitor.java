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
 * @author Martyn Cutcher
 */
public abstract class Visitor extends FilterBase {

	//-------------------------------------------------------------

    @Override
    final public Iterator filterOnce(Iterator src, Object context) {
        return new Visitorator(src, context, this);
	}

	protected abstract void visit(Object obj);
}
