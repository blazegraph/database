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
/*
 * Created on Jul 20, 2012
 */
package cutthecrap.utils.striterators;

/**
 * Interface for objects which can have resources which must be explicitly
 * closed (typically iterators).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ICloseable {

    /**
     * Closes the object, releasing any associated resources. This method MAY be
     * invoked safely if the object is already closed. Implementations of this
     * interface MUST invoke {@link #close()} once it is known that the object
     * will no longer be used.
     */
    public void close();
    
}
