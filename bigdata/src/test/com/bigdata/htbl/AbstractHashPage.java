/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Dec 1, 2010
 */
package com.bigdata.htbl;

import java.lang.ref.Reference;

import org.apache.log4j.Logger;

import com.bigdata.btree.PO;

/**
 * Abstract class for both directory and data pages for a hash index.
 */
abstract public class AbstractHashPage <T extends AbstractHashPage
/*
 * DO-NOT-USE-GENERIC-HERE. The compiler will fail under Linux (JDK 1.6.0_14,
 * _16).
 */
> extends PO //implements IAbstractNode, IAbstractNodeData 
{
	
	private final static transient Logger log = Logger
			.getLogger(AbstractHashPage.class);
	
	/**
	 * Transient back reference to the index to which this directory belongs.
	 */
	protected transient ExtensibleHashMap htbl;

	/**
	 * <p>
	 * A {@link Reference} to this page. This is created when the page is
	 * created and effectively provides a canonical {@link Reference} object for
	 * any given page.
	 * </p>
	 * 
	 * @todo Do we need back references for recursive directories?
	 */
    transient protected final Reference<? extends AbstractHashPage<T>> self;

    /**
     * Disallowed.
     */
    private AbstractHashPage() {

        throw new UnsupportedOperationException();
        
    }

	protected AbstractHashPage(final ExtensibleHashMap htbl, final boolean dirty) {

		if(htbl == null)
			throw new IllegalArgumentException();
		
		this.htbl = htbl;

        // reference to self: reused to link parents and children.
        this.self = htbl.newRef(this);
        
        if (!dirty) {

            /*
             * Nodes default to being dirty, so we explicitly mark this as
             * clean. This is ONLY done for the de-serialization constructors.
             */

            setDirty(false);

        }
        
//        @todo Add to the hard reference queue.
//        btree.touch(this);

	}
	
}
