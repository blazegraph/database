/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Sep 3, 2008
 */

package com.bigdata.journal;

import java.lang.ref.WeakReference;

import com.bigdata.rawstore.Bytes;

/**
 * Helper class for {@link IIndexStore#getTempStore()}. This class is very
 * light weight.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStoreFactory {

    private WeakReference<TemporaryStore> ref = null;
    
    private final long maxExtent;

    /**
     * The default maximum extent ({@value #DEFAULT_MAX_EXTENT}). A new
     * {@link TemporaryStore} will be created by {@link #getTempStore()} when
     * the extent of the current {@link TemporaryStore} reaches this value.
     */
    protected static final long DEFAULT_MAX_EXTENT = 5 * Bytes.gigabyte;
    
    public TemporaryStoreFactory() {

        this(DEFAULT_MAX_EXTENT);
        
    }

    /**
     * 
     * @param maxExtent
     *            The maximum extent of the current {@link TemporaryStore}
     *            before {@link #getTempStore()} will return a new
     *            {@link TemporaryStore}.
     * 
     * @throws IllegalArgumentException
     *             if <i>maxExtent</i> is negative (zero is allowed and will
     *             cause each request to return a distinct
     *             {@link TemporaryStore}).
     */
    public TemporaryStoreFactory(long maxExtent) {
        
        if (maxExtent < 0L)
            throw new IllegalArgumentException();
        
        this.maxExtent = maxExtent;
        
    }
    
    synchronized public TemporaryStore getTempStore() {

        TemporaryStore t = ref == null ? null : ref.get();

        if (t == null || t.getBufferStrategy().getExtent() > maxExtent) {

            t = new TemporaryStore();

            ref = new WeakReference<TemporaryStore>(t);

        }

        return t;
    }

}
