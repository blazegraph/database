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
import java.util.Date;

import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.SuccessorUtil;

/**
 * A schema for a sparse row store. Note that more than one schema may be used
 * with the same index. The name of the schema is always encoded as the
 * component of the key.
 * 
 * @todo support optional strong typing for column values?
 * @todo support optional required columns?
 * @todo verify schema name primary key names are legit (column and schema name
 *       checker utility).
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
     * 
     * @param primaryKeyType
     */
    public Schema(String name, String primaryKey, KeyType primaryKeyType) {

        NameChecker.assertSchemaName(name);
        
        NameChecker.assertColumnName(primaryKey);

        if (primaryKeyType == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.primaryKey = primaryKey;
        
        this.primaryKeyType = primaryKeyType;

        // one time encoding of the name of the schema.
        schemaBytes = KeyBuilder.asSortKey(name);

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
    public String getPrimaryKey() {
        
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

        return schemaBytes;

    }
 
    /**
     * The length of the value that will be returned by {@link #getSchemaBytes()}
     */
    final protected int getSchemaBytesLength() {

        return schemaBytes.length;
        
    }

    /*
     * Key builder stuff.
     */
    
    /**
     * Helper method appends a typed value to the compound key (this is used to
     * get the primary key into the compound key).
     * 
     * @param keyType
     *            The target data type.
     * @param v
     *            The value.
     * 
     * @return The {@link #keyBuilder}.
     * 
     * @todo support variable length byte[]s as a primary key type.
     * 
     * FIXME Verify that variable length primary keys do not cause problems in
     * the total ordering. Do we need a code (e.g., nul nul) that never occurs
     * in a valid primary key when the primary key can vary in length? The
     * problem occurs when the column name could become confused with the
     * primary key in comparisons owing to the lack of an unambiguous delimiter
     * and a variable length primary key. Another approach is to fill the
     * variable length primary key to a set length...
     */
    final protected IKeyBuilder appendPrimaryKey(IKeyBuilder keyBuilder, Object v, boolean successor) {
        
        KeyType keyType = getPrimaryKeyType();
        
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
                return keyBuilder.appendText(v.toString(), true/*unicode*/, true/*successor*/);
            case ASCII:
                return keyBuilder.appendText(v.toString(), false/*unicode*/, true/*successor*/);
            case Date:
                return keyBuilder.append(successor(keyBuilder,((Date) v).getTime()));
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
                return keyBuilder.appendText(v.toString(),true/*unicode*/,false/*successor*/);
            case ASCII:
                return keyBuilder.appendText(v.toString(),true/*unicode*/,false/*successor*/);
            case Date:
                return keyBuilder.append(((Date) v).getTime());
            }
            
        }

        return keyBuilder;
        
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
        
        KeyType keyType = getPrimaryKeyType();
        
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
            /*
             * Note: See toKey() for how to correctly form the sort key for the
             * successor of a Unicode value.
             */
            throw new UnsupportedOperationException();
//            return SuccessorUtil.successor(v.toString());
//            return SuccessorUtil.successor(v.toString());
        case Date:
            return SuccessorUtil.successor(((Date)v).getTime());
        }

        return keyBuilder;
        
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
     */
    final protected IKeyBuilder fromKey(IKeyBuilder keyBuilder,Object primaryKey) {
        
        keyBuilder.reset();

        // append the (encoded) schema name.
        keyBuilder.append(getSchemaBytes());
        
        // append the (encoded) primary key.
        appendPrimaryKey(keyBuilder,primaryKey,false/*successor*/);
        
        return keyBuilder;

    }

    /**
     * Forms the key in {@link #keyBuilder} that should be used as the last key
     * (exclusive) for a range query that will visit all index entries for the
     * specified primary key.
     * 
     * @param primaryKey
     *            The primary key.
     * 
     * @return The {@link #keyBuilder}, which will have the schema and the
     *         successor of the primary key already formatted in its buffer.
     */
    final protected IKeyBuilder toKey(IKeyBuilder keyBuilder,Object primaryKey) {
        
        keyBuilder.reset();

        // append the (encoded) schema name.
        keyBuilder.append(getSchemaBytes());

        // append successor of the (encoded) primary key.
        appendPrimaryKey(keyBuilder,primaryKey, true/*successor*/);

        return keyBuilder;

    }

    
    private final transient short VERSION0 = 0x0;
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        final short version = in.readShort();

        if (version != VERSION0)
            throw new IOException("Unknown version=" + version);

        name = in.readUTF();

        primaryKey = in.readUTF();
        
        primaryKeyType = (KeyType) in.readObject();
        
        // one time encoding of the name of the schema.
        schemaBytes = KeyBuilder.asSortKey(name);
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        out.writeShort(VERSION0);
        
        out.writeUTF(name);
        
        out.writeUTF(primaryKey);
        
        out.writeObject(primaryKeyType);
        
    }
    
}
