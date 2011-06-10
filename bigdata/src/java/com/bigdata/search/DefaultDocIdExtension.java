/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 10, 2011
 */

package com.bigdata.search;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderExtension;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;

/**
 * An implementation which provides backward compatibility with the use of
 * <code>long</code> document identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultDocIdExtension implements IKeyBuilderExtension<Long>{

    public int byteLength(Long obj) {
        
        return Bytes.SIZEOF_LONG;
        
    }

    public Long decode(final byte[] key, final int off) {
        
        return KeyBuilder.decodeLong(key, off);
        
    }

    public void encode(IKeyBuilder keyBuilder, Long obj) {
        
        keyBuilder.append(obj.longValue());
        
    }

}
