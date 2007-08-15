package com.bigdata.sparse;

import com.bigdata.btree.UnicodeKeyBuilder;

/**
 * A schema for a sparse row store defines. Note that more than one schema may
 * be used with the same index. The name of the schema is always encoded as the
 * component of the key.
 * 
 * @todo support optional strong typing for column values?
 * @todo support optional required columns?
 * @todo verify schema name primary key names are legit (column and schema name
 *       checker utility).
 */
public class Schema {
    
    private final String name;
    private final String primaryKey;
    private final KeyType primaryKeyType;
    private final byte[] schemaBytes;
    
    /**
     * 
     * @param name The schema name.
     * @param primaryKey
     *            The name of the column whose value is the (application
     *            defined) primary key.
     * 
     * @param primaryKeyType
     */
    public Schema(String name, String primaryKey,KeyType primaryKeyType) {

        NameChecker.assertSchemaName(name);
        
        NameChecker.assertColumnName(primaryKey);

        if (primaryKeyType == null)
            throw new IllegalArgumentException();
        
        this.name = name;
        
        this.primaryKey = primaryKey;
        
        this.primaryKeyType = primaryKeyType;

        // one time encoding of the name of the schema.
        schemaBytes = new UnicodeKeyBuilder().reset().append(name).getKey();

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
     * The UTF-8 encoding of the schema name.
     */
    public byte[] getSchemaBytes() {

        return schemaBytes;

    }
 
    /**
     * The length of the UTF-8 encoding of the schema name.
     */
    public int getSchemaBytesLength() {

        return schemaBytes.length;
        
    }
    
}
