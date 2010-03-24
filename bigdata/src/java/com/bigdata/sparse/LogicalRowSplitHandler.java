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
 * Created on Mar 17, 2009
 */
package com.bigdata.sparse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.IndexSegment;

/**
 * This class imposes the constraint that the separator key must not split a
 * logical. This constraint is required in order for the logical row store to
 * retain its row-wise ACID semantics when there is more than one shard for that
 * row store. This is done using a linear scan forward from the recommended
 * splitAt index until the first tuple is identified which would be part of a
 * different logical row. The index of that tuple is returned.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LogicalRowSplitHandler implements ISimpleSplitHandler, Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 9180840621078374197L;
    
    protected static transient final Logger log = Logger
            .getLogger(LogicalRowSplitHandler.class);

    public byte[] getSeparatorKey(final IndexSegment seg, final int fromIndex,
            final int toIndex, final int splitAt) {

        final byte[] a = seg.keyAt(splitAt);

        final int alen = new KeyDecoder(a).getPrefixLength();

        for (int i = splitAt + 1; i < toIndex; i++) {

            final byte[] b = seg.keyAt(i);

            final int blen = new KeyDecoder(b).getPrefixLength();

            /*
             * Compare the first N bytes of those keys (unsigned byte[]
             * comparison).
             */
            final int cmp = BytesUtil.compareBytesWithLenAndOffset(//
                    0/* aoff */, alen, a,//
                    0/* boff */, blen, b//
                    );

            // the keys must be correctly ordered.
            assert cmp <= 0;

            if (cmp < 0) {

                /*
                 * The N byte prefix has changed. Clone the first N bytes of the
                 * current key and return them to the caller. This is the
                 * minimum length first successor of the recommended key which
                 * can serve as a separator key without breaking the logical
                 * row.
                 */

                final byte[] prefix = new byte[blen];

                System.arraycopy(b/* src */, 0/* srcPos */, prefix/* dest */,
                        0/* destPos */, blen/* length */);

                if (log.isInfoEnabled())
                    log.info("Found: prefix=" + BytesUtil.toString(prefix)
                            + ", splitAt=" + splitAt + ", i=" + i);

                return prefix;

            }

        }

        log.warn("No successor: splitAt=" + splitAt);

        // No such successor!
        return null;

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        // NOP
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        // NOP
        
    }

}
