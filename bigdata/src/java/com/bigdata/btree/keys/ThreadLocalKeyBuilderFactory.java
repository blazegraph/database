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
 * Created on Jul 3, 2008
 */

package com.bigdata.btree.keys;

import com.bigdata.journal.Journal;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThreadLocalKeyBuilderFactory implements IKeyBuilderFactory {

    private final IKeyBuilderFactory delegate;
    
    public ThreadLocalKeyBuilderFactory(IKeyBuilderFactory delegate) {
        
        if (delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
    }
    
    /**
     * A {@link ThreadLocal} variable providing access to thread-specific
     * instances of a {@link IKeyBuilder} as configured by the delegate
     * {@link IKeyBuilderFactory}.
     * <p>
     * Note: this {@link ThreadLocal} is not static since we need configuration
     * properties from the constructor - those properties can be different for
     * different {@link Journal}s on the same machine.
     */
    private ThreadLocal<IKeyBuilder> threadLocalKeyBuilder = new ThreadLocal<IKeyBuilder>() {

        protected synchronized IKeyBuilder initialValue() {

            return delegate.getKeyBuilder();

        }

    };

    /**
     * Return a {@link ThreadLocal} {@link IKeyBuilder} instance configured
     * using the {@link IKeyBuilderFactory} specified to the ctor.
     */
    public IKeyBuilder getKeyBuilder() {
        
        return threadLocalKeyBuilder.get();
        
    }

}
