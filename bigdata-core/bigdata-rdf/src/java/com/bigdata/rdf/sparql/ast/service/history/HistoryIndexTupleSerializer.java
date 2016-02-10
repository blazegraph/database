/*

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
 * Created on Jun 23, 2008
 */

package com.bigdata.rdf.sparql.ast.service.history;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.util.Bytes;

/**
 * (De-)serializes {@link IChangeRecord}s for the history index.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/607"> History
 *      Service</a>
 *      
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: SPOTupleSerializer.java 5281 2011-10-03 14:00:39Z thompsonbry $
 */
public class HistoryIndexTupleSerializer extends
        DefaultTupleSerializer<HistoryChangeRecord, HistoryChangeRecord> {

    private static final long serialVersionUID = -1L;
    
    /**
     * The natural order for the {@link ISPO} data in the index (this is not a
     * total key order since the key has a revision timestamp prefix).
     */
    private SPOKeyOrder keyOrder;
    
    /**
     * If true, explicit SPOs decoded from index tuples will have a sid
     * attached.
     */
    private boolean sids;
    
    /**
     * De-serialization constructor.
     */
    public HistoryIndexTupleSerializer() {
        
    }

    /**
     * Create an {@link ITupleSerializer} for the indicated access path.
     * 
     * @param keyOrder
     *            The key order for the (s,p,o[,c]) component of the key.
     */
    public HistoryIndexTupleSerializer(final SPOKeyOrder keyOrder, final boolean sids) {

        this(keyOrder, sids, getDefaultLeafKeysCoder(), getDefaultValuesCoder());

    }
    
    /**
     * Create an {@link ITupleSerializer} for the indicated access path.
     * 
     * @param keyOrder
     *            The access path.
     * @param sids
     *            If true, attach sids to decoded SPOs where appropriate.
     * @param leafKeySer
     * @param leafValSer
     */
    public HistoryIndexTupleSerializer(final SPOKeyOrder keyOrder,
            final boolean sids, final IRabaCoder leafKeySer,
            final IRabaCoder leafValSer) {

        super(new ASCIIKeyBuilderFactory(), leafKeySer, leafValSer);

        this.keyOrder = keyOrder;

        this.sids = sids;

    }

    @Override
    public byte[] serializeKey(final Object obj) {

        if (obj == null)
            throw new IllegalArgumentException();
        
        if (obj instanceof HistoryChangeRecord)
            return serializeKey((HistoryChangeRecord) obj);

        throw new UnsupportedOperationException();
        
    }

    public byte[] serializeKey(final HistoryChangeRecord changeRecord) {

        final IKeyBuilder keyBuilder = getKeyBuilder().reset();

        // append the revision time.
        keyBuilder.append(changeRecord.getRevisionTime());

        // append the statement (encoded IVs).
        keyOrder.appendKey(keyBuilder, changeRecord.getStatement());

        final byte[] key = keyBuilder.getKey();

        return key;
        
    }

    @Override
    public byte[] serializeVal(final HistoryChangeRecord changeRecord) {

        if (changeRecord == null)
            throw new IllegalArgumentException();

        final ISPO spo = changeRecord.getStatement();

        final boolean override = spo.isOverride();

        final boolean userFlag = spo.getUserFlag();

        final StatementEnum type = spo.getStatementType();

        // optionally set the override and user flag bits on the value.
        final byte lowNibble = (byte) //
            (type.code()// statement type
                | (override ? StatementEnum.MASK_OVERRIDE : 0x0) //
                | (userFlag ? StatementEnum.MASK_USER_FLAG : 0x0)//
                );
                
        final byte highNibble = (byte) changeRecord.getChangeAction().ordinal();

        final byte b = (byte) (0xff & (highNibble << 4 | lowNibble));

        return new byte[] { b };

	}

    public HistoryChangeRecord deserializeKey(final ITuple tuple) {
        
        // just de-serialize the whole tuple.
        return deserialize(tuple);
        
    }

	public HistoryChangeRecord deserialize(final ITuple tuple) {

        if (tuple == null)
            throw new IllegalArgumentException();
        
        // copy of the key in a reused buffer.
        final byte[] key = tuple.getKeyBuffer().array();

        // Decode the revision timestamp.
        final long revisionTimestamp = KeyBuilder.decodeLong(key, 0/* off */);

        // Decode statement, starting after the revision timestamp.
        final SPO spo = keyOrder.decodeKey(key, Bytes.SIZEOF_LONG/* off */);

        final ChangeAction changeAction;
        if ((tuple.flags() & IRangeQuery.VALS) != 0) {

            /*
             * Decode the statement type, bit flags, and optional sid based on
             * the tuple value.
             */

            final byte b = tuple.getValueBuffer().array()[0];

            // Just the low nibble. 
            final byte lowNibble = (byte) (0xff & (b & 0x0f));

            // Just the high nibble. 
            final byte highNibble = (byte) (0xff & (b >> 4));

            {
                
                final byte code = lowNibble;

                final StatementEnum type = StatementEnum.decode(code);

                spo.setStatementType(type);

                spo.setOverride(StatementEnum.isOverride(code));

                spo.setUserFlag(StatementEnum.isUserFlag(code));

                if (sids) {

                    // SIDs only valid for triples.
                    assert keyOrder.getKeyArity() == 3;

                    if (spo.isExplicit()) {

//                        spo.setStatementIdentifier(true);

                    }

                }
            
            }

            changeAction = ChangeAction.values()[highNibble];

        } else {
            
            // Not available unless VALS are read.
            changeAction = null;
            
        }

        final HistoryChangeRecord changeRecord = new HistoryChangeRecord(spo,
                changeAction, revisionTimestamp);

        return changeRecord;

    }

    /**
     * The initial version.
     */
    private final static transient byte VERSION0 = 0;

    /**
     * The current version.
     */
    private final static transient byte VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        final byte version = in.readByte();
        
        switch (version) {
        case VERSION0:
            keyOrder = SPOKeyOrder.valueOf(in.readByte());
            sids = in.readByte() > 0;
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }


    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        super.writeExternal(out);

        out.writeByte(VERSION);

        out.writeByte(keyOrder.index());
        
        out.writeByte(sids ? 1 : 0);

    }

}
