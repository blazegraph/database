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
 * Created on Jun 13, 2011
 */

package com.bigdata.search;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.IKeyBuilderExtension;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.search.FullTextIndex.Options;

/**
 * Class manages the encoding and decoding of keys for the full text index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FullTextIndexTupleSerializer<V extends Comparable<V>> extends
        DefaultTupleSerializer<ITermDocKey<V>, ITermDocVal> {

    private boolean fieldsEnabled;
    private boolean doublePrecision;
    private IKeyBuilderExtension<V> docIdFactory;

    /**
     * 
     */
    public FullTextIndexTupleSerializer() {
    }

    /**
     * @param keyBuilderFactory
     *            This factory governs the Unicode collation order that will be
     *            imposed on the indexed tokens.
     * @param leafKeysCoder
     *            The coder used for the leaf keys (prefix coding is fine).
     * @param leafValsCoder
     *            The coder used for the leaf values (custom coding may provide
     *            tighter representations of the {@link ITermDocVal}s in the
     *            index entries).
     * @param fieldsEnabled
     *            When <code>true</code> the <code>fieldId</code> will be
     *            included as a component in the generated key. When
     *            <code>false</code> it will not be present in the generated
     *            key.
     * @param doublePrecision
     *            When <code>true</code>, the term weight will be serialized
     *            using double precision.
     */
    public FullTextIndexTupleSerializer(//
            final IKeyBuilderFactory keyBuilderFactory,//
            final IRabaCoder leafKeysCoder, //
            final IRabaCoder leafValsCoder,//
            final boolean fieldsEnabled,//
            final boolean doublePrecision,//
            final IKeyBuilderExtension<V> docIdFactory//
            ) {
   
        super(keyBuilderFactory, leafKeysCoder, leafValsCoder);

        this.fieldsEnabled = fieldsEnabled;
        this.doublePrecision = doublePrecision;
        this.docIdFactory = docIdFactory;
        
    }

    public ITermDocKey<V> deserializeKey(final ITuple tuple) {

//        final byte[] key = tuple.getKeyBuffer().array();
//
//        return IVUtility.decode(key);
        throw new UnsupportedOperationException();
    }

    public byte[] serializeKey(final Object obj) {

//        return ((TermId<?>) obj).encode(getKeyBuilder().reset()).getKey();
        throw new UnsupportedOperationException();

    }

    public byte[] serializeVal(final ITermDocVal obj) {
        
//        buf.reset();
//        
//        return valueSer.serialize(obj, buf, tbuf);
        throw new UnsupportedOperationException();

    }

    public ITermDocRecord<V> deserialize(final ITuple tuple) {

//        final IV iv = deserializeKey(tuple);
//        
//        final BigdataValue tmp = valueSer.deserialize(tuple.getValueStream(),
//                new StringBuilder());
//
//        tmp.setIV(iv);
//
//        return tmp;
        throw new UnsupportedOperationException();

    }

    /**
     * The initial version.
     */
    private static final transient byte VERSION0 = 0;

    private static final transient byte VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        final byte version = in.readByte();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new IOException("unknown version=" + version);
        }
        this.fieldsEnabled = in.readBoolean();
        this.doublePrecision = in.readBoolean();
        
        {
            final String className = in.readUTF();
            final Class<IKeyBuilderExtension<V>> cls;
            try {
                cls = (Class<IKeyBuilderExtension<V>>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Bad option: "
                        + Options.DOCID_FACTORY_CLASS, e);
            }

            if (!IKeyBuilderExtension.class.isAssignableFrom(cls)) {
                throw new RuntimeException(Options.DOCID_FACTORY_CLASS
                        + ": Must extend: "
                        + IKeyBuilderExtension.class.getName());
            }

            try {

                final Constructor<? extends IKeyBuilderExtension<V>> ctor = cls
                        .getConstructor(new Class[] { /* FullTextIndex.class */});

                // save reference.
                docIdFactory = ctor.newInstance(new Object[] { /* this */});

            } catch (Exception ex) {

                throw new RuntimeException(ex);

            }
        }

    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeByte(VERSION);
        out.writeBoolean(fieldsEnabled);
        out.writeBoolean(doublePrecision);
        out.writeUTF(docIdFactory.getClass().getName());
    }

}
