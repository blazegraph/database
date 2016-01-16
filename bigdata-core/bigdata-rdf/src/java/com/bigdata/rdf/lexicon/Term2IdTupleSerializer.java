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
 * Created on Jul 7, 2008
 */

package com.bigdata.rdf.lexicon;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import org.openrdf.model.Value;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.TermId;

/**
 * Handles the term:id index (forward mapping for the lexicon). The keys are
 * unsigned byte[]s representing a total order for the RDF {@link Value} space.
 * The index assigns term identifiers, and those term identifiers are stored in
 * the values of the index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Term2IdTupleSerializer.java 4818 2011-06-29 20:01:56Z
 *          thompsonbry $
 */
public class Term2IdTupleSerializer extends DefaultTupleSerializer {

    /**
     * 
     */
    private static final long serialVersionUID = 1486882823994548034L;

    /**
     * De-serialization ctor.
     */
    public Term2IdTupleSerializer() {
        
        super();
        
    }
    
    /**
     * Configures the {@link IKeyBuilderFactory} from the caller's <i>properties</i>.
     * 
     * @param properties
     */
    public Term2IdTupleSerializer(final Properties properties) {
        
        this(new DefaultKeyBuilderFactory(properties));
        
    }

    /**
     * Uses the caller's {@link IKeyBuilderFactory}.
     * 
     * @param keyBuilderFactory
     */
    public Term2IdTupleSerializer(final IKeyBuilderFactory keyBuilderFactory) {

        /*
         * The FixedLengthValueRabaCoder is MUCH faster than the
         * CannonicalHuffman coder and takes the same space on the disk. An all
         * around win.
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/506 (Load,
         * closure and query performance in 1.1.x versus 1.0.x)
         */
//        super(keyBuilderFactory);
        super(keyBuilderFactory, getDefaultLeafKeysCoder(),
            IVUtility.PACK_TIDS //
            ? new com.bigdata.btree.raba.codec.SimpleRabaCoder()//
            : new com.bigdata.btree.raba.codec.FixedLengthValueRabaCoder(9)//
            );
        
    }

    /**
     * Thread-local object for constructing keys for the lexicon.
     */
    public LexiconKeyBuilder getLexiconKeyBuilder() {
        
        /*
         * FIXME We should save off a reference to this to reduce heap churn
         * and then use that reference in this class.
         */
        return new LexiconKeyBuilder(getKeyBuilder());
        
    }

    /**
     * Return a {@link LexiconKeyBuilder} that is setup with collation strength
     * PRIMARY.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/974" >
     *      Name2Addr.indexNameScan(prefix) uses scan + filter </a>
     */
    public LexiconKeyBuilder getLexiconPrimaryKeyBuilder() {
        
        /*
         * FIXME We should save off a reference to this to reduce heap churn
         * and then use that reference in this class.
         */
        return new LexiconKeyBuilder(getPrimaryKeyBuilder());
        
    }

    /**
     * You can not decode the term:id keys since they include Unicode sort keys
     * and that is a lossy transform.
     * 
     * @throws UnsupportedOperationException
     *             always
     */
    @Override
    public Object deserializeKey(ITuple tuple) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Return the unsigned byte[] key for an RDF {@link Value}.
     * 
     * @param obj
     *            The RDF {@link Value}.
     */
    @Override
    public byte[] serializeKey(Object obj) {

        return getLexiconKeyBuilder().value2Key((Value)obj);
        
    }

    /**
     * Return the byte[] value, which is a term identifier written as a packed
     * long integer.
     * 
     * @param obj
     *            A term identifier expressed as a {@link TermId}.
     */
    @Override
    public byte[] serializeVal(final Object obj) {

        final IV<?,?> iv = (IV<?,?>) obj;

        /*
         * Note: reusing the same KeyBuilder as the keys, but that is Ok since
         * the IV encoding does not rely on the Unicode properties and the 
         * KeyBuilder is a thread-local instance so there is no contention for
         * it.
         */
        final byte[] key = iv.encode(getKeyBuilder()).getKey();

        return key;
        
    }

    /**
     * De-serializes the {@link ITuple} as a {@link IV} whose value is the
     * term identifier associated with the key. The key itself is not decodable.
     */
    @Override
    public IV deserialize(final ITuple tuple) {

        final ByteArrayBuffer b = tuple.getValueBuffer();
        
        return IVUtility.decodeFromOffset(b.array(), 0);
        
    }

    /**
     * The initial version (no additional persistent state).
     */
    private final static transient byte VERSION0 = 0;

    /**
     * The current version.
     */
    private final static transient byte VERSION = VERSION0;

    @Override
    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        super.readExternal(in);
        
        final byte version = in.readByte();
        
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }

    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {

        super.writeExternal(out);
        
        out.writeByte(VERSION);
        
    }
    
}
