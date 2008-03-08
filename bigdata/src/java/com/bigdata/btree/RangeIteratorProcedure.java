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
 * Created on Jan 28, 2008
 */

package com.bigdata.btree;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.IIndexProcedure.IKeyRangeIndexProcedure;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.IDataService;

/**
 * Procedure for key range scan of an index.
 * 
 * @deprecated This procedure has not been worked through at the
 *             {@link ClientIndexView} level. Instead the range iterator is
 *             handled as a method call on the {@link IDataService} unlike
 *             nearly every other remote operation on an index. I have not
 *             yet decided whether it makes sense to re-implement the range
 *             iterator as an {@link IKeyRangeIndexProcedure}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RangeIteratorProcedure extends AbstractKeyRangeIndexProcedure
        implements IReadOnlyOperation{

    /**
     * 
     */
    private static final long serialVersionUID = -9182517329697177725L;
    
    protected int capacity;
    protected int flags;
    protected ITupleFilter filter;
    
    /**
     * De-serialization ctor.
     */
    public RangeIteratorProcedure() {
    }

    /**
     * @param fromKey
     * @param toKey
     */
    public RangeIteratorProcedure(byte[] fromKey, byte[] toKey) {

        this(fromKey, toKey, 0/* capacity */, IRangeQuery.KEYS
                | IRangeQuery.VALS/* flags */, null/* filter */);
        
    }

    /**
     * Designated variant (the one that gets overriden) for an iterator that
     * visits the entries in a half-open key range.
     * 
     * @param fromKey
     *            The first key that will be visited (inclusive). When
     *            <code>null</code> there is no lower bound.
     * @param toKey
     *            The first key that will NOT be visited (exclusive). When
     *            <code>null</code> there is no upper bound.
     * @param capacity
     *            The #of entries to buffer at a time. This is a hint and MAY be
     *            zero (0) to use an implementation specific <i>default</i>
     *            capacity. The capacity is intended to limit the burden on the
     *            heap imposed by the iterator if it needs to buffer data, e.g.,
     *            before sending it across a network interface.
     * @param flags
     *            A bitwise OR of {@link #KEYS} and/or {@link #VALS} determining
     *            whether the keys or the values or both will be visited by the
     *            iterator.
     * @param filter
     *            An optional filter and/or resolver.
     * 
     * @see #entryIterator(), which visits all entries in the btree.
     * 
     * @see SuccessorUtil, which may be used to compute the successor of a value
     *      before encoding it as a component of a key.
     * 
     * @see BytesUtil#successor(byte[]), which may be used to compute the
     *      successor of an encoded key.
     * 
     * @see EntryFilter, which may be used to filter the entries visited by the
     *      iterator.
     */
    public RangeIteratorProcedure(byte[] fromKey, byte[] toKey, int capacity,
            int flags, ITupleFilter filter) {

        super(fromKey, toKey);
        
    }

    public Object apply(IIndex ndx) {
        
        return newResultSet(ndx);
        
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);

        capacity = in.readInt();

        flags = in.readInt();

        filter = (ITupleFilter) in.readObject();

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        writeKeys(out);

        out.writeInt(capacity);

        out.writeInt(flags);

        out.writeObject(filter);
        
    }

    /**
     * Return the object that will materialize and serialize the result.
     * <p>
     * Note: You MAY override this method in order to customize the
     * serialization behavior for the keys and/or values in the
     * {@link ResultSet}.
     * 
     * @param ndx
     *            The index.
     */
    protected ResultSet newResultSet(IIndex ndx) {
        
        return new ResultSet( ndx, fromKey, toKey, capacity, flags, filter );
        
    }

}
