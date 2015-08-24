/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Jun 29, 2012
 */
package com.bigdata.btree;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Iterator visits index entries (dereferencing visited tuples to the
 * application objects stored within those tuples).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public final class EntryScanIterator implements ICloseableIterator {

    private final ITupleIterator src;

    public EntryScanIterator(final ITupleIterator src) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;

    }

    @Override
    public boolean hasNext() {
        return src.hasNext();
    }

    @Override
    public Object next() {

        final ITuple t = src.next();

        // Resolve to the index entry.
        return t.getObject();

    }

    @Override
    public void remove() {
        src.remove();
    }

    @Override
    public void close() {
        // NOP
    }

}
