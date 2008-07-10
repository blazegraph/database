package com.bigdata.relation.locator;

import com.bigdata.relation.IRelation;
import com.bigdata.sparse.KeyType;
import com.bigdata.sparse.Schema;

/**
 * A {@link Schema} for metadata about existing relations. Each relation
 * corresponds more or less to a table in an RDBMS. A relation "exists" when
 * there are indices for its data. An {@link IRelation} is a Class that
 * allows you to read or write on the relation's data. Given the
 * {@link #NAMESPACE} of a relation, the {@link RelationSchema} specifies
 * the {@link #CLASS} that should be instantiated when an {@link IRelation}
 * instance is created to operation on some relation's data. The schema is
 * freely extensible and may be used to store additional properties that are
 * made available to the {@link IRelation} when a new instance of that
 * {@link Class} is instantiated. The relation namespace keys are Unicode.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RelationSchema extends Schema {

    /**
     * 
     */
    private static final long serialVersionUID = -558566998386484688L;

    /**
     * The name of the property whose value is the namespace of the
     * {@link ILocatableResource} (this is the primary key).
     */
    public static final String NAMESPACE = "namespace";

    /**
     * The name of the property whose value is the name of the {@link Class}
     * used to instantiate the {@link ILocatableResource}.
     */
    public static final String CLASS = "class";

    /**
     * The name of the property whose value is namespace of the container (if
     * any) for this the {@link ILocatableResource} resource having {@link #NAMESPACE}
     * as its resource identifer. When defined, this value MUST be a prefix of
     * the value stored under the {@link #NAMESPACE} property.
     */
    public static final String CONTAINER = "container";

    /**
     * A shared instance.
     */
    public transient static final RelationSchema INSTANCE = new RelationSchema();
    
    /**
     * 
     * @see #INSTANCE
     */
    protected RelationSchema() {

        super("__relation", NAMESPACE, KeyType.Unicode);

    }

}
