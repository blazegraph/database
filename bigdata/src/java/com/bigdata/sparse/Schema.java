/*

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
package com.bigdata.sparse;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.util.Date;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;

/**
 * A schema for a sparse row store. Note that more than one schema may be used
 * with the same index. The name of the schema is always encoded as the first
 * component of the key.
 * 
 * @todo support optional strong typing for column values?
 * @todo support optional required columns?
 */
public class Schema implements Externalizable {
    
    /**
     * 
     */
    private static final long serialVersionUID = -7619134292028385179L;
    
    private String name;
    private String primaryKey;
    private KeyType primaryKeyType;
    private transient byte[] schemaBytes;
    
    /**
     * De-serialization ctor.
     */
    public Schema() {
        
    }
    
    /**
     * 
     * @param name
     *            The schema name.
     * @param primaryKey
     *            The name of the column whose value is the (application
     *            defined) primary key.
     * @param primaryKeyType
     *            The data type for the primary key.
     */
    public Schema(final String name, final String primaryKey,
            final KeyType primaryKeyType) {

        NameChecker.assertSchemaName(name);
        
        NameChecker.assertColumnName(primaryKey);

        if (primaryKeyType == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.primaryKey = primaryKey;
        
        this.primaryKeyType = primaryKeyType;
        
        // eager computation of the encoded scheme name.
        getSchemaBytes();

    }

    /**
     * The name of the schema.
     */
    public String getName() {
        
        return name;
        
    }
    
    /**
     * The name of the column whose value is the primary key.
     */
    public String getPrimaryKeyName() {
        
        return primaryKey;
        
    }
    
    /**
     * The data type that is used for the primary key when forming the total
     * key.
     */
    public KeyType getPrimaryKeyType() {
        
        return primaryKeyType;
        
    }
    
    /**
     * The Unicode sort key encoding of the schema name.
     */
    final protected byte[] getSchemaBytes() {

        if (schemaBytes == null) {

            /*
             * One time encoding of the schema name as a Unicode sort key.
             */
        
            schemaBytes = KeyBuilder.asSortKey(name);
            
//            schemaBytes = KeyBuilder.newInstance().append(name).append("\0").getKey();

        }
        
        return schemaBytes;

    }
 
//    /**
//     * The length of the value that will be returned by {@link #getSchemaBytes()}
//     */
//    final protected int getSchemaBytesLength() {
//
//        return getSchemaBytes().length;
//        
//    }

    /*
     * Key builder stuff.
     */
    
    /**
     * Helper method appends a typed value to the compound key (this is used to
     * get the primary key into the compound key).
     * <p>
     * Note: This automatically appends the <code>nul</code> terminator byte
     * if {@link KeyType#isFixedLength()} is <code>false</code>. That byte
     * flags the end of the primary key for variable length primary keys.
     * 
     * @param keyType
     *            The target data type.
     * @param v
     *            The value.
     * 
     * @return The {@link #keyBuilder}.
     * 
     * @see KeyDecoder
     */
    final protected IKeyBuilder appendPrimaryKey(IKeyBuilder keyBuilder, Object v, boolean successor) {
        
        final KeyType keyType = getPrimaryKeyType();
        
        if (successor) {
            
            switch (keyType) {

            case Integer:
                return keyBuilder.append(successor(keyBuilder,((Number) v).intValue()));
            case Long:
                return keyBuilder.append(successor(keyBuilder,((Number) v).longValue()));
            case Float:
                return keyBuilder.append(successor(keyBuilder,((Number) v).floatValue()));
            case Double:
                return keyBuilder.append(successor(keyBuilder,((Number) v).doubleValue()));
            case Unicode:
                return keyBuilder.appendText(v.toString(), true/*unicode*/, true/*successor*/).appendNul();
            case ASCII:
                return keyBuilder.appendText(v.toString(), false/*unicode*/, true/*successor*/).appendNul();
            case Date:
                return keyBuilder.append(successor(keyBuilder,((Date) v).getTime()));
//            case UnsignedBytes:
//                return keyBuilder.append((byte[])v).appendNul();
            default:
                throw new UnsupportedOperationException();
            }
            
        } else {
            
            switch (keyType) {

            case Integer:
                return keyBuilder.append(((Number) v).intValue());
            case Long:
                return keyBuilder.append(((Number) v).longValue());
            case Float:
                return keyBuilder.append(((Number) v).floatValue());
            case Double:
                return keyBuilder.append(((Number) v).doubleValue());
            case Unicode:
                return keyBuilder.appendText(v.toString(),true/*unicode*/,false/*successor*/).appendNul();
            case ASCII:
                return keyBuilder.appendText(v.toString(),false/*unicode*/,false/*successor*/).appendNul();
            case Date:
                return keyBuilder.append(((Date) v).getTime());
//            case UnsignedBytes:
//                return keyBuilder.append((byte[]) v);
            default:
                throw new UnsupportedOperationException();
            }
            
        }

//        return keyBuilder;
        
    }
 
    /**
     * Return the successor of a primary key object.
     * 
     * @param v
     *            The object.
     * 
     * @return The successor.
     * 
     * @throws UnsupportedOperationException
     *             if the primary key type is {@link KeyType#Unicode}. See
     *             {@link #toKey(Object)}, which correctly forms the successor
     *             key in all cases.
     */
    final private Object successor(IKeyBuilder keyBuilder,Object v) {
        
        final KeyType keyType = getPrimaryKeyType();
        
        switch(keyType) {

        case Integer:
            return SuccessorUtil.successor(((Number)v).intValue());
        case Long:
            return SuccessorUtil.successor(((Number)v).longValue());
        case Float:
            return SuccessorUtil.successor(((Number)v).floatValue());
        case Double:
            return SuccessorUtil.successor(((Number)v).doubleValue());
        case Unicode:
        case ASCII:
//        case UnsignedBytes:
            /*
             * Note: See toKey() for how to correctly form the sort key for the
             * successor of a Unicode value.
             */
            throw new UnsupportedOperationException();
//            return SuccessorUtil.successor(v.toString());
//            return SuccessorUtil.successor(v.toString());
        case Date:
            return SuccessorUtil.successor(((Date) v).getTime());
        default:
            throw new UnsupportedOperationException();
        }
//
//        return keyBuilder;
        
    }
    
    /**
     * Forms the key in {@link #keyBuilder} that should be used as the first key
     * (inclusive) for a range query that will visit all index entries for the
     * specified primary key.
     * 
     * @param primaryKey
     *            The primary key.
     * 
     * @return The {@link #keyBuilder}, which will have the schema and the
     *         primary key already formatted in its buffer.
     * 
     * @see KeyDecoder
     */
    final protected IKeyBuilder fromKey(IKeyBuilder keyBuilder,Object primaryKey) {
        
        keyBuilder.reset();

        // append the (encoded) schema name.
        keyBuilder.append(getSchemaBytes());
        keyBuilder.append(primaryKeyType.getByteCode());
        keyBuilder.appendNul();

        if (primaryKey != null) {

            // append the (encoded) primary key.
            appendPrimaryKey(keyBuilder, primaryKey, false/* successor */);

        }
        
        return keyBuilder;

    }

    /**
     * The prefix that identifies all tuples in the logical row for this schema
     * having the indicated value for their primary key.
     * 
     * @param primaryKey
     *            The value of the primary key for the logical row.
     * 
     * @return
     */
    final public byte[] getPrefix(IKeyBuilder keyBuilder,Object primaryKey) {
        
        return fromKey(keyBuilder, primaryKey).getKey();
        
    }
    
    /*
     * Note: use SuccessorUtil.successor(key.clone()) instead.
     */
    
//    /**
//     * Forms the key in {@link #keyBuilder} that should be used as the last key
//     * (exclusive) for a range query that will visit all index entries for the
//     * specified primary key.
//     * 
//     * @param primaryKey
//     *            The primary key.
//     * 
//     * @return The {@link #keyBuilder}, which will have the schema and the
//     *         successor of the primary key already formatted in its buffer.
//     * 
//     * @see KeyDecoder
//     * 
//     * @throws IllegalArgumentException
//     *             if either argument is <code>null</code>.
//     */
//    final protected IKeyBuilder toKey(IKeyBuilder keyBuilder, Object primaryKey) {
//
//        if (primaryKey == null)
//            throw new IllegalArgumentException();
//        
//        keyBuilder.reset();
//
//        // append the (encoded) schema name.
//        keyBuilder.append(getSchemaBytes());
//        keyBuilder.append(primaryKeyType.getByteCode());
//        keyBuilder.appendNul();
//
//        // append successor of the (encoded) primary key.
//        appendPrimaryKey(keyBuilder, primaryKey, true/* successor */);
//
//        return keyBuilder;
//
//    }

    /**
     * Encodes a key for the {@link Schema}.
     * 
     * @param keyBuilder
     * @param primaryKey
     *            The primary key for the logical row (required).
     * @param col
     *            The column name (required).
     * @param timestamp
     *            The timestamp (required).
     * 
     * @return The encoded key.
     * 
     * @throws IllegalArgumentException
     *             if <i>keyBuilder</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>primaryKey</i> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>col</i> is not valid as the name of a column.
     */
    public byte[] getKey(IKeyBuilder keyBuilder, Object primaryKey, String col,
            long timestamp) {

        if (keyBuilder == null)
            throw new IllegalArgumentException();

        if (primaryKey == null)
            throw new IllegalArgumentException();
        
        NameChecker.assertColumnName(col);
        
        // encode the schema name and the primary key.
        fromKey(keyBuilder, primaryKey);

        /*
         * The column name. Note that the column name is NOT stored with Unicode
         * compression so that we can decode it without loss.
         */
        try {

            keyBuilder.append(col.getBytes(SparseRowStore.UTF8)).appendNul();
            
        } catch(UnsupportedEncodingException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        keyBuilder.append(timestamp);

        final byte[] key = keyBuilder.getKey();
        
        return key;

    }
    
    private final static transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        final short version = in.readShort();

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        name = in.readUTF();

        primaryKey = in.readUTF();
        
        primaryKeyType = KeyType.getKeyType(in.readByte());
        
        // eager computation of the encoded scheme name.
        getSchemaBytes();

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        out.writeShort(VERSION0);
        
        out.writeUTF(name);
        
        out.writeUTF(primaryKey);
        
        out.writeByte(primaryKeyType.getByteCode());
        
    }

    public String toString() {
        
        return "Schema{name=" + getName() + ",primaryKeyName=" + getPrimaryKeyName()
                + ",primaryKeyType=" + getPrimaryKeyType() + "}";
        
    }
    
}
