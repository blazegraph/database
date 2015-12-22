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
 * Created on Jun 25, 2008
 */

package cutthecrap.utils.striterators;

import java.util.Iterator;

/**
 * An iterator that defines a {@link #close()} method - you MUST close instances
 * of this interface. Many implementation depends on this in order to release
 * resources, terminate tasks, etc.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface ICloseableIterator<E> extends Iterator<E>, ICloseable {

    /**
     * Closes the iterator, releasing any associated resources. This method MAY
     * be invoked safely if the iterator is already closed. Implementations of
     * this interface MUST invoke {@link #close()} if {@link Iterator#hasNext()}
     * method returns <code>false</code> to ensure that the iterator is closed
     * (and its resources release) as soon as it is exhausted.
     * <p>
     * Note: Implementations that support {@link Iterator#remove()} MUST NOT
     * eagerly close the iterator when it is exhausted since that would make it
     * impossible to remove the last visited statement. Instead they MUST wait
     * for an explicit {@link #close()} by the application.
     */
	@Override
    public void close();
    
}
