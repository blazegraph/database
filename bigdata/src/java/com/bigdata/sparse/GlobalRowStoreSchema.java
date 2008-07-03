package com.bigdata.sparse;


/**
 * Schema for the {@link SparseRowStore} used as a global row store for named
 * property sets within the federation. This schema only declares the
 * primaryKey, {@link #NAME}, and a single property value, {@link #VALUE}.
 * <p>
 * Applications may simply declare their own property names or they may declare
 * their own {@link Schema}. Applications are responsible for ensuring that the
 * primaryKey values are globally distinct, e.g., by use of a namespace
 * convention.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GlobalRowStoreSchema extends Schema {

    private static final long serialVersionUID = -4880904728328862689L;

    /**
     * The name of the property whose value is the name of the object (this
     * is the primary key).
     */
    public static final String NAME = "name";

    /**
     * The name of the property whose value is the object associated with
     * the name.
     */
    public static final String VALUE = "value";

    /**
     * A shared instance.
     */
    public transient static final GlobalRowStoreSchema INSTANCE = new GlobalRowStoreSchema();
    
    /**
     * 
     * @see #INSTANCE
     */
    public GlobalRowStoreSchema() {

        super("__base", NAME, KeyType.Unicode);

    }

}
