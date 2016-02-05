/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Aug 17, 2009
 */

package com.bigdata.btree.raba.codec;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.raba.AbstractRaba;

/**
 * Abstract implementation throws {@link UnsupportedOperationException} for all
 * mutation operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractCodedRaba implements ICodedRaba {

    protected AbstractCodedRaba() {
        
    }

    /**
     * Implementation is read-only.
     */
    @Override
    final public boolean isReadOnly() {

        return true;

    }

    @Override
    final public int add(byte[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    final public int add(byte[] value, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    final public int add(DataInput in, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    final public void set(int index, byte[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    final public String toString() {

        return AbstractRaba.toString(this);

    }

    /**
     * Basic implementation may be overridden if a faster implementation is
     * available.
     */
    @Override
    public Iterator<byte[]> iterator() {

        return new Iterator<byte[]>() {

            int i = 0;

            @Override
            public boolean hasNext() {

                return i < size();

            }

            @Override
            public byte[] next() {

                if (!hasNext())
                    throw new NoSuchElementException();

                return get(i++);

            }

            @Override
            public void remove() {

                throw new UnsupportedOperationException();

            }

        };

    }

}
