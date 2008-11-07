/*
 * Created on Sep 29, 2008
 */

package com.bigdata.rdf.sail;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.openrdf.sail.SailException;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.ScaleOutTripleStore;
import com.bigdata.relation.locator.RelationSchema;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.jini.JiniServicesHelper;

/**
 * Class provides guidence on parameter setup a data set and queries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataSailHelper {

    /**
     * (Re-)open a SAIL backed by persistent data stored in an
     * {@link IBigdataFederation}.
     * 
     * @param fed
     *            The federation.
     * @param namespace
     *            The namespace of the triple store.
     * @param timestamp
     *            The timestamp of the view.
     * 
     * @return The SAIL.
     */
    public BigdataSail getSail(IBigdataFederation fed, String namespace,
            long timestamp) {

        ScaleOutTripleStore tripleStore = (ScaleOutTripleStore) fed
                .getResourceLocator().locate(namespace, timestamp);

        if (tripleStore == null) {

            if (timestamp == ITx.UNISOLATED) {

                // create a new triple store.

                System.out.println("Creating tripleStore: namespace="
                        + namespace);

                tripleStore = new ScaleOutTripleStore(fed, namespace,
                        timestamp, getProperties());

                tripleStore.create();

            } else {

                throw new RuntimeException("No such triple store: namespace="
                        + namespace + ", timestamp=" + timestamp);

            }

        }

        return new BigdataSail(tripleStore);

    }
    
    /**
     * (Re-)open a SAIL backed by persistent data stored on a {@link Journal}.
     * 
     * @param filename
     *            The name of the backing file for the {@link Journal}.
     * @param namespace
     *            The namespace of the triple store.
     * @param timestamp
     *            The timestamp of the view.
     *            
     * @return The SAIL.
     */
    public BigdataSail getSail(String filename, String namespace, long timestamp) {
    
        final Properties properties = new Properties();
        
        properties.setProperty(Options.FILE, filename);
        
        final Journal journal = new Journal(properties);
        
        System.err.println("createTime="+journal.getRootBlockView().getCreateTime());
        System.err.println("lastCommitTime="+journal.getLastCommitTime());
        
        LocalTripleStore tripleStore = (LocalTripleStore) journal
                .getResourceLocator().locate(namespace, timestamp);
        
        if (tripleStore == null) {

            if (timestamp == ITx.UNISOLATED) {

                // create a new triple store.

                System.out.println("Creating tripleStore: namespace="
                        + namespace);

                tripleStore = new LocalTripleStore(journal, namespace,
                        timestamp, getProperties());

                tripleStore.create();

            } else {

                throw new RuntimeException("No such triple store: namespace="
                        + namespace + ", timestamp=" + timestamp);
                
            }
            
        }
        
        return new BigdataSail(tripleStore);
        
    }
    
    /**
     * Return the {@link Properties} that will be used to configure a new
     * {@link AbstractTripleStore} instance. The {@link AbstractTripleStore}
     * will remember the properties with which it was created and use those
     * values each time it is re-opened. The properties are stored in the global
     * row store for the backing {@link IIndexManager}.
     * <p>
     * Note: You need to edit this code to correspond to your application
     * requirements. Currently the code reflects an application using a triple
     * store without inference, without statement level provenance, and with the
     * full text index enabled. Another common configuration is a triple store
     * with RDFS++ inference, which can be realized by changing the
     * {@link AbstractTripleStore.Options#AXIOMS_CLASS} property value and
     * possibly enabling {@link Options#TRUTH_MAINTENANCE} depending on whether
     * or not you will be incrementally or bulk loading data.
     * 
     * @todo Bundle some sample properties files which will make it easier for
     *       people to configure a {@link BigdataSail} and make that a command
     *       line option. The jini configuration approach is nice since it
     *       allows us to use symbolic constants in the configuration files.
     */
    public Properties getProperties() {

        final Properties properties = new Properties();
        
        /*
         * Override the initial and maximum extent so that they are more suited
         * to large data sets.
         */
        properties.setProperty(Options.INITIAL_EXTENT,""+200*Bytes.megabyte);
        properties.setProperty(Options.MAXIMUM_EXTENT,""+200*Bytes.megabyte);
        
        /*
         * Turn off truth maintenance since there are no entailments for the
         * ontology.
         */
        properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");

        /*
         * Disable RDFS++ inference since there are no entailments for the
         * ontology.
         */
        properties.setProperty(Options.AXIOMS_CLASS,NoAxioms.class.getName());
        
        /*
         * Enable rewrites of high-level queries into native rules (native JOIN
         * execution).
         */
        properties.setProperty(BigdataSail.Options.NATIVE_JOINS, "true");

        /*
         * Maximum #of subqueries to evaluate concurrently for the 1st join
         * dimension for native rules. Zero disables the use of an executor
         * service. One forces a single thread, but runs the subquery on the
         * executor service. N>1 is concurrent subquery evaluation.
         */
        properties.setProperty(Options.MAX_PARALLEL_SUBQUERIES, "0");
      
        /*
         * The #of elements that will be materialized at a time from an access
         * path. The default is 20,000. However, the relatively selective joins,
         * small result sets for the queries, and the use of LIMIT with a FILTER
         * (a regex in this case) outside of the LIMIT means that a smaller
         * chunk size will cause the JOIN evaluation to do MUCH less work. If
         * the default chunk size is used, then queries with LIMIT or OFFSET can
         * take much longer to evaluate since they are computing a large #of
         * solutions beyond those required to satisify the limit (after applying
         * the regex FILTER).
         */
        properties.setProperty(Options.CHUNK_CAPACITY, "100");
      
//      properties.setProperty(Options.FULLY_BUFFERED_READ_THRESHOLD, "10000");

        /*
         * Turn off incremental closure in the DataLoader object.
         */
        properties.setProperty(
                com.bigdata.rdf.store.DataLoader.Options.CLOSURE,
                DataLoader.ClosureEnum.None.toString());

        /*
         * Turn off commit in the DataLoader object. We do not need to commit
         * anything until we have loaded all the data and computed the closure
         * over the database.
         */
        properties.setProperty(
                com.bigdata.rdf.store.DataLoader.Options.COMMIT,
                DataLoader.CommitEnum.None.toString());

        /*
         * Turn off Unicode support for index keys (this is a big win for load
         * rates since LUBM does not use Unicode data, but it has very little
         * effect on query rates since the only time we generate Unicode sort
         * keys is when resolving the Values in the queries to term identifiers
         * in the database).
         */
//        properties.setProperty(Options.COLLATOR, CollatorEnum.ASCII.toString());

        /*
         * Leave the full text index enabled since it will be used to answer
         * the search queries. 
         */
//        /*
//         * Turn off the full text index.
//         */
//        properties.setProperty(Options.TEXT_INDEX, "false");

        /*
         * Turn off statement identifiers (provenance mode).
         */
        properties.setProperty(Options.STATEMENT_IDENTIFIERS, "false");

        /*
         * Turn off justifications (impacts only the load performance, but it is
         * a big impact and only required if you will be doing TM). (Actually,
         * since there are no entailments this will have no effect).
         */
        properties.setProperty(Options.JUSTIFY, "false");
        
        return properties;
        
    }

    /**
     * Return the properties associated with the {@link AbstractTripleStore}
     * backing the {@link BigdataSail}.
     * 
     * @param namespace
     *            The sail.
     *            
     * @return The persistent properties.
     */
    public Properties getProperties(BigdataSail sail) {

        return getProperties(sail.getDatabase().getIndexManager(), sail
                .getDatabase().getNamespace());
        
    }
    
    /**
     * Return the properties associated with the given namespace.
     * 
     * @param indexManager
     *            Use {@link BigdataSail#getDatabase()} and then
     *            {@link AbstractTripleStore#getIndexManager()}.
     * @param namespace
     *            The namespace of a locatable resource such as an
     *            {@link AbstractTripleStore}, {@link SPORelation} or
     *            {@link LexiconRelation}.
     * 
     * @return The persistent properties.
     */
    protected Properties getProperties(IIndexManager indexManager,
            String namespace) {

        Map<String, Object> map = indexManager.getGlobalRowStore().read(
                RelationSchema.INSTANCE, namespace);

        Properties properties = new Properties();

        properties.putAll(map);

        return properties;

    }

    /**
     * Update properties for the SAIL. This will overwrite any properties having
     * the same name with their new values. Properties that are not overwritten
     * will remain visible. A property can be deleted by specifying a
     * <code>null</code> value.
     * <p>
     * Note: this changes the persistent property values associated with the
     * SAIL. It DOES NOT change the properties associated with the given
     * instance. You MUST re-open the SAIL in order for the new properties to be
     * in effect.
     * <p>
     * Note: While many property values can be changed dynamically, some may
     * not. In particular, the properties that effect the way in which the keys
     * for the indices are generated as stored within the indices themselves.
     * Among other things this ensures that Unicode configuration options are
     * applied uniformly when an is accessed by any host in a federation.
     * 
     * @param sail
     *            The SAIL.
     * @param properties
     *            The properties.
     *            
     *            @return The post-modification properties.
     */
    public Properties setProperties(BigdataSail sail, Properties properties) {

        return setProperties(sail.getDatabase().getIndexManager(), sail
                .getDatabase().getNamespace(), properties);
        
    }
    
    /**
     * 
     * @param indexManager
     * @param namespace
     * @param properties
     * @return The post-modification properties.
     */
    protected Properties setProperties(IIndexManager indexManager, String namespace,
            Properties properties) {

        /*
         * Convert the Properties to a Map.
         */
        final Map<String, Object> map = new HashMap<String, Object>();
        {

            // set the namespace (primary key).
            map.put(RelationSchema.NAMESPACE, namespace);
            
            final Enumeration<? extends Object> e = properties.propertyNames();

            while (e.hasMoreElements()) {

                final Object key = e.nextElement();

                final String name = (String) key;
                
                map.put(name, properties.getProperty(name));

            }

        }

        /*
         * Write the map on the row store. This will overwrite any entries for
         * the same properties. Properties that are not overwritten will remain
         * visible.
         */
        final Properties p2 = new Properties();

        p2.putAll( indexManager.getGlobalRowStore().write(RelationSchema.INSTANCE, map) );

        if(indexManager instanceof Journal) {

            // make the changes restart safe (not required for federations).
            ((Journal)indexManager).commit();

        }
        
        // return the post-modification properties.
        return p2;
        
    }

    protected static void showProperties(Properties p) {
        
        // sorted collection.
        final TreeMap<String/*name*/, Object/*val*/> map = new TreeMap<String,Object>();

        // put into alpha order.
        for(Map.Entry<Object,Object> entry : p.entrySet()) {
            
            map.put(entry.getKey().toString(), entry.getValue());
            
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {

            System.out.println(entry.getKey() + "=" + entry.getValue());

        }
        
    }
    
    public static void main(String[] args) throws SailException {
        
        final String filename = args[0];
        
        final String namespace = args.length > 1 ? args[1] : "kb";

        final long timestamp = args.length > 2 ? Long.valueOf(args[2]) : ITx.UNISOLATED;
        
        final BigdataSailHelper helper = new BigdataSailHelper();
        
        System.out.println("filename: "+filename);
        
        final BigdataSail sail;
        final JiniServicesHelper jiniServicesHelper;
        
        if(!new File(filename).isDirectory()) {

            // Presume a simple Journal.
            
            sail = helper.getSail(filename, namespace, timestamp);
            
            jiniServicesHelper = null;
            
        } else {
            
            // Presume a Jini config directory (@todo also handle LDS/EDS).
            
            jiniServicesHelper = new JiniServicesHelper(filename);
            
            jiniServicesHelper.start();
            
            final IBigdataFederation fed = jiniServicesHelper.client.connect(); 
            
            sail = helper.getSail(fed, namespace, timestamp);
            
        }

        try {

            sail.initialize();

            System.out.println("\npre-modification properties::");
            showProperties(helper.getProperties(sail));

            // change some property values.
            {
                final Properties p = new Properties();

                p.setProperty(Options.CHUNK_CAPACITY, "100");
                p.setProperty(Options.MAX_PARALLEL_SUBQUERIES, "0");
//                p.setProperty(Options.INCLUDE_INFERRED, "true");
//                p.setProperty(Options.QUERY_TIME_EXPANDER, "true");

                System.out.println("\npost-modification properties::");
                showProperties(helper.setProperties(sail, p));
            }

        } finally {

            sail.shutDown();

            if (jiniServicesHelper != null) {
                
                jiniServicesHelper.shutdown();
                
            }
            
        }

    }

}
