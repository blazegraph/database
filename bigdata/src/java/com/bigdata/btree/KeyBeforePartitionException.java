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
 * Created on Dec 6, 2008
 */

package com.bigdata.btree;

import java.io.File;

import com.bigdata.mdi.LocalPartitionMetadata;

/**
 * Exception thrown when a key is before the start of the half-open range of an
 * index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class KeyBeforePartitionException extends KeyOutOfRangeException {

    /**
     * 
     */
    private static final long serialVersionUID = 6985672103255764765L;

    /**
     * @param key
     * @param allowUpperBound
     * @param pmd
     * @param storeFile
     */
    public KeyBeforePartitionException(final byte[] key,
            final boolean allowUpperBound, final LocalPartitionMetadata pmd,
            final File storeFile) {

        super("key=" + BytesUtil.toString(key) + ", allowUpperBound="
                + allowUpperBound + ", pmd=" + pmd + ", storeFile=" + storeFile);
        
    }

}
