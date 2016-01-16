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
 * Created on Oct 4, 2011
 */

package com.bigdata.rdf.lexicon;

import com.bigdata.btree.FixedLengthPrefixSplits;

/**
 * Split handler enforces the constraint that a collision bucket is never split
 * across a shard boundary. This is a necessary constraint. If we were to permit
 * the collision bucket to be split across a shard boundary, then the scan for a
 * matching entry in the BLOBS index will no longer be shard local and therefore
 * would no longer be ACID (that is, it will be busted). The constraint is
 * enforced by requiring the separator key to create a distinction in the prefix
 * bytes which exclude the collision counter.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlobsIndexSplitHandler extends FixedLengthPrefixSplits {
    
    public BlobsIndexSplitHandler() {
        super(BlobsIndexHelper.TERMS_INDEX_KEY_SIZE
                - BlobsIndexHelper.SIZEOF_COUNTER);
    }

}
