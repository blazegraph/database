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
 * Created on Nov 23, 2008
 */

package com.bigdata.config;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.RelationSchema;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.util.NV;

/**
 * Base class for managing the initial configuration metadata for indices and
 * locatable resources.
 * 
 * @todo There are some drawbacks with this approach. It remains to be seen
 *       whether this can be improved on readily.
 *       <p>
 *       We can not report properties that DO NOT correspond to any known
 *       property within the umbrella bigdata namespace (as log4j does) because
 *       we do not make a closed world assumption for properties in that
 *       namespace.
 *       <p>
 *       We can not interpret properties given in a Java code style (as jini
 *       does with its Configuration object).
 *       <p>
 *       We are not using the Java beans model so you can not describe property
 *       values or instantiate objects using reflection. Instead, the logic for
 *       that stuff shows up in the code for the class that is being configured.
 *       <p>
 *       This presumes a fixed syntactic relation between a resource/index and
 *       its container rather than the explicit relation defined by
 *       {@link RelationSchema#CONTAINER}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Configuration {

    /**
     * Property values are logged at INFO.
     */
    protected static final transient Logger log = Logger.getLogger(Configuration.class);
    
    /**
     * The prefix for namespace specific property value overrides.
     */
    public static final transient String NAMESPACE = "com.bigdata.namespace";

    /**
     * The namespace separator character.
     */
    public static final transient char DOT = '.';
    
    /**
     * Return the value for property, which may be the default value, a global
     * override, or a namespace override. Defaults are assigned by three
     * mechanisms.
     * <ol>
     * 
     * <li>Default values are generally described in the javadoc for
     * <code>Options</code> interfaces. The specific default is supplied by
     * the caller and will be used if the value is not overriden using any of
     * the other methods.</li>
     * 
     * <li>The default value may be globally overriden using the property name.
     * For example, you can override the default branching factor for all
     * {@link BTree}s by specifying a value for
     * {@link IndexMetadata.Options#BTREE_BRANCHING_FACTOR}. In general, the
     * name of the property is declared by an interface along with its default
     * value. </li>
     * 
     * <li>Any value may be overriden by a value that is specific to the
     * <i>namespace</i> (or to any prefix of that <i>namespace</i> which can
     * be formed by chopping off the namespace at a {@link #DOT}). For example,
     * you can override the branching factor property for an index named
     * <code>foo.myIndex</code> by specifying a value for the property name
     * <code>com.bigdata.namespace.foo.myIndex.com.bigdata.btree.BTree.branchingFactor</code> ({@value #NAMESPACE}
     * is the {@link #NAMESPACE} prefix for overrides, <code>foo.myIndex</code>
     * is the name of the index, and
     * {@value IndexMetadata.Options#BTREE_BRANCHING_FACTOR} is the name of the
     * property that will be overriden for that index). Alternatively you can
     * override the branching factor for all indices in the "foo" relation by
     * specifying a value for the property name
     * <code>com.bigdata.namespace.foo.com.bigdata.btree.BTree.branchingFactor</code>.
     * Note: You can use {@link #getOverrideProperty(String, String)} to form
     * these property names automatically, including from within a Jini
     * configuration file.</li>
     * 
     * </ol>
     * 
     * @param indexManagerIsIgnored
     *            The value specified to the ctor (optional).
     * @param properties
     *            The properties object against which the value of the property
     *            will be resolved.
     * @param namespace
     *            The namespace of the index, relation, etc (optional).
     * @param propertyName
     *            The bare name of the property whose default value is requested
     *            (without the namespace).
     * @param defaultValue
     *            The value for that property that will be returned if the
     *            default has not been overriden as described above (optional).
     * 
     * @return The resolved value for the property.
     * 
     * @todo test when namespace is empty (journal uses that) and possibly null.
     */
    public static String getProperty(final IIndexManager indexManagerIsIgnored,
            final Properties properties, final String namespace,
            final String propertyName, final String defaultValue) {
    
        final NV nv = getProperty2(indexManagerIsIgnored, properties, namespace,
                propertyName, defaultValue);
        
        if(nv == null) return null;
        
        return nv.getValue();
        
    }
    
    /**
     * Variant returns both the name under which the value was discovered and
     * the value.
     * 
     * @param indexManagerIsIgnored
     * @param properties
     * @param namespace
     * @param globalName
     * @param defaultValue
     * @return
     */
    public static NV getProperty2(final IIndexManager indexManagerIsIgnored,
            final Properties properties, final String namespace,
            final String globalName, final String defaultValue) {

        // indexManager MAY be null.
        if (properties == null)
            throw new IllegalArgumentException();
//        if (namespace == null)
//            throw new IllegalArgumentException();
        if (globalName == null)
            throw new IllegalArgumentException();
        // defaultValue MAY be null.
        
        String key = null;
        String val = null;

        final String localName = globalName;//getLocalName(globalName);

        /*
         * Look for a namespace match, or a match on any prefix of the namespace
         * which can be formed by chopping off the last remaining component in
         * the namespace.
         */
        if (namespace != null) {
            
            // right size the buffer.
            final StringBuilder sb = new StringBuilder(NAMESPACE.length() + 1
                    + namespace.length() + 1 + localName.length());

            /*
             * Check the full namespace on the first pass then chop off the last
             * remaining component of each successive pass.
             */
            String prefix = namespace;
            
            while (prefix.length() > 0) {

                sb.setLength(0); // reset each time.
                sb.append(NAMESPACE);
                sb.append(DOT);
                sb.append(prefix);
                sb.append(DOT);
                sb.append(localName);

                // namespace override.
                val = properties.getProperty(key = sb.toString());

                if (val != null) {

                    // Match - will be logged below.
                    break;
                    
                }
                
                if (log.isDebugEnabled())
                    log.debug("No match: " + key);

                final int lastIndexOf = prefix.lastIndexOf(DOT);
                
                if (lastIndexOf == -1) {

                    // No match.
                    break;
                    
                }
                
                // chop off the last component and try again.
                prefix = prefix.substring(0, lastIndexOf);

            }
            
        }
        
        if (val == null) {

            // global override.
            val = properties.getProperty(key = globalName);
            
            if( val == null) {
            
                // no override.
                val = defaultValue;
                
            }

        }

        if (log.isInfoEnabled())
            log.info(key + "=" + val);
        
        return new NV(key, val);

    }

    /**
     * Variant converts to the specified generic type and validates the value.
     * 
     * @param <E>
     * @param indexManager
     * @param properties
     * @param namespace
     * @param globalName
     * @param defaultValue
     * @param validator
     * 
     * @return The validated value -or- <code>null</code> if there was no
     *         default.
     */
    public static <E> E getProperty(final IIndexManager indexManager,
            final Properties properties, final String namespace,
            final String globalName, final String defaultValue,
            final IValidator<E> validator)
            throws ConfigurationException {
    
        if (validator == null)
            throw new IllegalArgumentException();
        
        final NV nv = getProperty2(indexManager, properties, namespace,
                globalName, defaultValue);

        if (nv == null)
            return null;
        
        final E e = validator.parse(nv.getName(), nv.getValue());

        validator.accept(nv.getName(), nv.getValue(), e);
        
        return e;
        
    }
    
//    /**
//     * Return the last component of the globalName.
//     * <p>
//     * Note: If '.' does not appear, then lastIndexOf == -1 and beginIndex :=
//     * lastIndexOf + 1 == 0, so the localName will be the same as the
//     * globalName.
//     * 
//     * @param globalName
//     *            The global name of some property.
//     */
//    static protected String getLocalName(String globalName) {
//
//        final int lastIndexOf = globalName.lastIndexOf(DOT);
//
//        final String localName = globalName.substring(lastIndexOf + 1);
//
//        return localName;
//            
//    }
    
    /**
     * Resolve the value to a {@link DataService} {@link UUID}.
     * 
     * @param indexManager
     *            The index manager (optional).
     * @param val
     *            The value is either a {@link UUID} or a service name.
     * 
     * @return The {@link UUID} of the identified service -or- <code>null</code>
     *         if no service is identified for that value or if the
     *         <i>indexManager</i> is either not given or not an
     *         {@link IBigdataFederation}.
     * 
     * @throws IllegalArgumentException
     *             if the <i>val</i> is <code>null</code>.
     */
    static protected UUID resolveDataService(final IIndexManager indexManager,
            final String val) {

        if (indexManager == null)
            return null;

        if (val == null)
            throw new IllegalArgumentException();

        if (!(indexManager instanceof IBigdataFederation))
            return null;
        
        final IBigdataFederation fed = ((IBigdataFederation) indexManager);

        /*
         * Value is a UUID?
         */
        try {

            // valid UUID?
            return UUID.fromString(val);

        } catch (IllegalArgumentException ex) {

            // Ignore.

        }

        /*
         * Value is the name of a data service?
         */
        {
         
            final IDataService dataService = fed.getDataServiceByName(val);

            if (dataService != null) {

                try {

                    return dataService.getServiceUUID();
                    
                } catch (IOException ex) {
                    
                    throw new RuntimeException(ex);
                    
                }
                
            }

            // fall through.
            
        }
        
        // can't interpret the value.
        
        log.warn("Could not resolve: "+val);
        
        return null;

    }

    /**
     * Return the name that can be used to overrride the specified property for
     * the given namespace.
     * 
     * @param namespace
     *            The namespace (of an index, relation, etc).
     * @param property
     *            The global property name.
     *            
     * @return The name that is used to override that property for that
     *         namespace.
     */
    public static String getOverrideProperty(final String namespace,
            final String property) {
        
        final String override = NAMESPACE + DOT + namespace + DOT + property;
        
        if(log.isInfoEnabled()) {
            
            log.info("namespace=" + namespace + ", property=" + property
                    + ", override=" + override);

        }

        return override;

    }

}
