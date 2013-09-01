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

/**
 * Interface for managing push/pop of context in a striterator.
 */
public interface IContextMgr {

    /**
     * Hook gives the implementation an opportunity to push context onto a
     * stack.
     * 
     * @param context
     *            The context object.
     */
    void pushContext(Object context);

    /**
     * Hook gives the implementation an opportunity to pop context off of a
     * stack.
     */
	void popContext();

}
