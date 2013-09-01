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
 * An iterator wrapper which does not support {@link #remove()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyIterator<E> implements Iterator<E> {

    private final Iterator<E> src;

    public ReadOnlyIterator(final Iterator<E> src) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;

    }

    @Override
    public boolean hasNext() {
        return src.hasNext();
    }

    @Override
    public E next() {
        return src.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
