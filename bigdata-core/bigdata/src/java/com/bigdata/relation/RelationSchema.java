package com.bigdata.relation;

import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.ILocatableResource;
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
    public static final String NAMESPACE = RelationSchema.class.getPackage()
            .getName()
            + ".namespace";

    /**
     * The name of the property whose value is the name of the {@link Class}
     * used to instantiate the {@link ILocatableResource}.
     */
    public static final String CLASS = RelationSchema.class.getPackage()
            .getName()
            + ".class";

    /**
     * The name of the property whose value is namespace of the container (if
     * any) for this the {@link ILocatableResource} resource having {@link #NAMESPACE}
     * as its resource identifier. When defined, this value MUST be a prefix of
     * the value stored under the {@link #NAMESPACE} property.
     */
    public static final String CONTAINER = RelationSchema.class.getPackage()
            .getName()
            + ".container";

    /**
     * A dynamically injected property which can reveal the commit time from
     * which a locatable resource was materialized.
     * 
     * @see DefaultResourceLocator
     * @see AbstractResource
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/266
     * 
     *      TODO This is a workaround for and should be removed when we replace
     *      the native long tx identifier with a thin interface.
     */
    public static final String COMMIT_TIME = (RelationSchema.class.getPackage()
            .getName() + ".commitTime").intern(); 
    
    /**
     * A shared instance.
     */
    public transient static final RelationSchema INSTANCE = new RelationSchema();
    
    /**
     * De-serialization ctor.
     * 
     * @see #INSTANCE
     */
    public RelationSchema() {

        super("__rel"/*RelationSchema.class.getName()*/, NAMESPACE, KeyType.Unicode);

    }

}
