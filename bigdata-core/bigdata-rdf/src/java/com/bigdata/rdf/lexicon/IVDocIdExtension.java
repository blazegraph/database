/**

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
 * Created on Jun 10, 2011
 */

package com.bigdata.rdf.lexicon;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.search.FullTextIndex;

/**
 * Implementation provides for the use of {@link IV}s in the
 * {@link FullTextIndex}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IVDocIdExtension implements IKeyBuilderExtension<IV> {

    public int byteLength(final IV obj) {

        return obj.byteLength();
        
    }

    public IV decode(final byte[] key, final int off) {
        
        return IVUtility.decodeFromOffset(key, off);
        
    }

    public void encode(final IKeyBuilder keyBuilder, final IV obj) {
        
        IVUtility.encode(keyBuilder, obj);
        
    }

}
