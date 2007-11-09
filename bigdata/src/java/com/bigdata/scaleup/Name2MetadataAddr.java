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
 * Created on Feb 9, 2007
 */

package com.bigdata.scaleup;

import com.bigdata.btree.BTreeMetadata;
import com.bigdata.journal.Name2Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * Extension of {@link Name2Addr} for locating the {@link MetadataIndex}
 * associated with a named {@link PartitionedIndexView}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Name2MetadataAddr extends Name2Addr {

    public Name2MetadataAddr(IRawStore store) {

        super(store);
        
    }
    
    /**
     * Deserialization constructor.
     * @param store
     * @param metadata
     */
    public Name2MetadataAddr(IRawStore store, BTreeMetadata metadata) {

        super(store,metadata);
        
    }

}
