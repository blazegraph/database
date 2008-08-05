/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 4, 2006
 */

package com.bigdata.gom;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.CognitiveWeb.extser.IExtensibleSerializer;
import org.CognitiveWeb.generic.IPropertyClass;
import org.CognitiveWeb.generic.core.AbstractBTree;
import org.CognitiveWeb.generic.core.IBlob;
import org.CognitiveWeb.generic.core.LinkSetIndex;
import org.CognitiveWeb.generic.core.PropertyClass;
import org.CognitiveWeb.generic.core.ndx.ByteSuccessor;
import org.CognitiveWeb.generic.core.ndx.CharacterSuccessor;
import org.CognitiveWeb.generic.core.ndx.Coercer;
import org.CognitiveWeb.generic.core.ndx.DefaultUnicodeCoercer;
import org.CognitiveWeb.generic.core.ndx.DoubleSuccessor;
import org.CognitiveWeb.generic.core.ndx.FloatSuccessor;
import org.CognitiveWeb.generic.core.ndx.IntegerSuccessor;
import org.CognitiveWeb.generic.core.ndx.LongSuccessor;
import org.CognitiveWeb.generic.core.ndx.ShortSuccessor;
import org.CognitiveWeb.generic.core.ndx.Successor;
import org.CognitiveWeb.generic.core.om.BaseObject;
import org.CognitiveWeb.generic.core.om.IPersistentStore;
import org.CognitiveWeb.generic.core.om.ObjectManager;
import org.CognitiveWeb.generic.core.om.blob.Blob;
import org.CognitiveWeb.generic.core.om.cache.ICacheEntry;
import org.CognitiveWeb.generic.core.om.cache.ICacheListener;
import org.CognitiveWeb.generic.core.om.cache.LRUCache;
import org.CognitiveWeb.generic.core.om.cache.WeakValueCache;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.ThreadLocalKeyBuilderFactory;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.rawstore.Bytes;

/**
 * Integration for bigdata.
 * 
 * FIXME integrate extSer. use unisolated reads and writes on the extSer index.
 *
 * @todo Support GOM pre-fetch using a rangeIterator - that will materialize N
 *       records on the client and could minimize trips to the server. I am not
 *       sure about unisolated operations for GOM.... Isolated operations are
 *       straight forward. The other twist is supporting scalable link sets,
 *       link set indices (not named, unless the identity of the object
 *       collecting the link set is part of the key), and non-OID indices
 *       (requires changes to generic-native). I think that link sets might have
 *       to become indices in order to scale (to break the cycle of updating
 *       both the object collecting the link set and the head/tail and
 *       prior/next members in the link set). Or perhaps all those could be
 *       materialized on the client and then an unisolated operation (perhaps
 *       with conflict resolution?!?) would persist the results...
 * 
 * @todo see gom-for-jdbm or generic-native for UML models.
 * 
 * @todo variant of extSer that supports even more compact serialization using
 *       mg4j bit output streams and bit coding of long identifiers using
 *       hu-tucker or non-alpha variant of hu-tucker.
 * 
 * @todo extser integration that supports scale-out.
 * 
 * @todo split distributed cache
 * 
 * @todo group objects locally with a generic object by inserting them into the
 *       same index using the generic object identifier (byte[]) as the base key
 *       and then appending a locally unique identifier.
 * 
 * @todo compare performance of rdf-generic with ctc vs bigdata.
 * 
 * @todo rename Generic to GPO and BaseObject to PO.
 * 
 * @todo refactor to support allocation of generic objects within indices other
 *       than the default object identifier index. generalizing to application
 *       choosen object indices will require that the object remembers the index
 *       from which it is deserialized or against which it was allocated. This
 *       is only transient state since it can be recovered when the object is
 *       deserialized.
 * 
 * @todo in order to support transparent index partitioning the link set indices
 *       need to be named indices registered with the journal otherwise the
 *       indices can not be located during overflow or or asynchronous
 *       partitioning tasks.
 *       <p>
 *       We do not need a key comparator, we just need to build the keys
 *       correctly.
 *       <p>
 *       this means we need to create the index when we register the index
 *       family and that the per-link set index is just a key range limited view
 *       of the btree. removing all entries for a specific link set is a
 *       key-range delete (an optimized method could be added to {@link BTree}
 *       for this). the index is only destroyed if the index family is
 *       unregistered. integrating things this way will require changing the
 *       {@link IPersistenceStore} interface.
 * 
 * @todo support collection of incremental operations and their batch execution
 *       on the server using the "native" transaction counter - this will be a
 *       drammatic performance boost since we will not have to make RPCs or go
 *       through the concurrency control mechanisms for object creation, object
 *       state changes, or object deletion. We can also pre-fetch a set of
 *       persistent objects at a time by using a key range scan rather than a
 *       point lookup - this will help with a distributed database. For the
 *       moment we can pre-assign the object identifiers from a counter, but the
 *       code should be modified to allow lazy conversion of persistence capable
 *       objects to persistent objects per the {@link BTree}.
 * 
 * @todo support the transaction model by associating a transaction identifier
 *       with an object manager. this means that the journal instance needs to
 *       be shared across the persistence store instances and that operations
 *       need to be submitted as tasks run against the concurrency model.
 *       <p>
 *       this will require changing the {@link IPersistentStore} interface. the
 *       create/close store and registering store private indices will need to
 *       be encapsulated by a different interface.
 *       <p>
 *       Note: the named objects should use isolation when the object manager is
 *       isolated.
 *       <p>
 *       Note: the string table should always use unisolated operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: PersistenceStore.java,v 1.21 2006/06/15 15:47:10 thompsonbry
 *          Exp $
 */
public class PersistenceStore implements IPersistentStore
{

    public static final Logger log = Logger.getLogger(PersistenceStore.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    boolean isINFO() {return log.getEffectiveLevel().toInt() <= Level.INFO.toInt();}

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    boolean isDEBUG() {return log.getEffectiveLevel().toInt() <= Level.DEBUG.toInt();}
    
    /**
     * Some additional options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     * @version $Id: PersistenceStore.java,v 1.2 2006/04/18 20:48:29 thompsonbry
     *          Exp $
     */
    public static class RuntimeOptions extends
            org.CognitiveWeb.generic.core.om.RuntimeOptions implements Options {

        /**
         * The default value of the {@link #FILE} property.
         */
        public static final String STORE_FILE_DEFAULT = "store"+Options.JNL;

        /**
         * The name of the optional property whose positive integer value is the
         * size of the LRU hard reference object cache backing the weak
         * reference object cache.
         * 
         * @todo rename to share the namespace for this across GOM native
         *       backends?
         */
        public static final String OBJECT_CACHE_SIZE = "om.bigdata.cacheSize";
        public static final String OBJECT_CACHE_SIZE_DEFAULT = "10000"; 

    }
    
    /**
     * The name of the {@link #oid_ndx}.
     */
    private static final String OID_NDX = "__oid_ndx";

    /**
     * The name of the {@link #name_ndx}.
     */
    private static final String NAME_NDX = "__name_ndx";

    /**
     * The name of the {@link #pcls_ndx}.
     */
    private static final String PCLS_NDX = "__pcls_ndx";
    
    /**
     * The name of the {@link #str_id_ndx}.
     */
    private static final String STR_ID_NDX = "__str_id_ndx";
    
    /**
     * The name of the {@link #id_str_ndx}.
     */
    private static final String ID_STR_NDX = "__id_str_ndx";
    
    /**
     * The object manager.
     */
    transient private final ObjectManager om;
    
    /**
     * The underlying store manager.
     */
    transient private AbstractJournal m_journal;

    /**
     * The {@link BTree} that maps object identifiers to persistent objects. The
     * keys are 64-bit long integers. The values are the persistent objects.
     * <p>
     * Note: This approach natually clusters persistent objects based on
     * insertion order. However, you can always interpret the oid as a byte[]
     * and then append additional bytes to force an object to be clustered
     * locally with another persistent object.
     */
    transient private BTree oid_ndx;

    /**
     * The {@link BTree} that maps names to persistent objects. The keys are
     * Unicode names. The values are the object identifiers.
     */
    transient private BTree name_ndx;

    /**
     * The {@link BTree} that maps names to the oid of the named property class.
     * The keys are Unicode names. The values are the object identifier of the
     * named property class.
     */
    transient private BTree pcls_ndx;
    
    /**
     * The {@link BTree} that maps strings to persistent integer assignments.
     * The keys are Unicode strings. The values are the 32-bit integers.
     */
    transient private BTree str_id_ndx;
    
    /**
     * Reverse map from identifer to string.
     */
    transient private BTree id_str_ndx;

    /**
     * Returns the underlying {@link IJournal} object.
     *
     * @exception IllegalStateException if the object manager is not
     * valid.
     */
    final public IJournal getJournal() {

        if (m_journal == null) {

            throw new IllegalStateException("Object manager is not valid.");

        }

        return m_journal;

    }

    /**
     * A thread-local {@link IKeyBuilder} used for the default object index
     * (formatting a long OID as an unsigned byte[] key).
     */
    final protected IKeyBuilder getKeyBuilder() {
        
        return keyBuilderFactory.getKeyBuilder();
        
    }

    private final ThreadLocalKeyBuilderFactory keyBuilderFactory;
    
    /**
     * Required constructor either opens the named store or creates a new store
     * if none exists with that name.
     * 
     * @param properties
     *            Specifies properties that can effect the behavior of the
     *            object manager. The following properties are defaulted if not
     *            specified:
     *            <ul>
     *            <li><code>jdbm.om.basename=store</code> The base filename
     *            for the underlying store file. Two files are created: one with
     *            the extension ".db" and one with the extension ".lg". If the
     *            store does not exist, then a new store is created. Otherwise
     *            the existing store is opened.</li>
     *            </ul>
     * 
     * @see #getJournal()
     * @see RuntimeOptions
     */
    public PersistenceStore( ObjectManager om, Properties properties ) {
        
    	if( om == null ) {
    		
            throw new IllegalArgumentException();
            
        }

        this.om = om;
        
        /*
         * We default some properties. Therefore, in order to avoid side effects
         * on the caller's [properties] object, we wrap it in another properties
         * object before modifying it.
         */

        properties = new Properties(properties);

        /* 
         * Default the file name iff the journal is to be backed by a file.
         */
        
        BufferMode bufferMode = BufferMode.valueOf(properties.getProperty(
                Options.BUFFER_MODE, Options.DEFAULT_BUFFER_MODE.toString()));
        
        boolean createTempFile = Boolean.parseBoolean(properties.getProperty(
                Options.CREATE_TEMP_FILE, ""+Options.DEFAULT_CREATE_TEMP_FILE));
        
        if (bufferMode.isStable() && !createTempFile
                && properties.getProperty(Options.FILE) == null) {
        
            properties.setProperty(Options.FILE,
                    RuntimeOptions.STORE_FILE_DEFAULT);
            
        }

        /*
         * @todo the property name map uses Unicode keys so the configuration
         * here will effect how that index recognizes distinct property names
         * and how it ordered properties. it should have an index local tuple
         * serializer with its own index local key builder factory. the OID
         * index should also use an index local key builder, but all it does is
         * map Long to unsigned byte[8]. The stringId map also uses this key
         * builder and should have its own configuration.
         * 
         * Note: The same key builder is used to map application attributes onto
         * keys for the link set indices. Those link set indices should be made
         * into scale-out indices per relation (link set family). It might be
         * possible for each to have its own configured tuple serializer and key
         * builder factory that handles the conversion of application objects
         * into internal keys, however duplicate keys are handled in a fairly
         * specialized manner so this needs to be looked into further.
         */
        keyBuilderFactory = new ThreadLocalKeyBuilderFactory(
                new DefaultKeyBuilderFactory(properties));
        
        /*
         * Initialize the object cache.
         */
        {
         
            final int objectCacheSize = Integer.parseInt(properties
                    .getProperty(RuntimeOptions.OBJECT_CACHE_SIZE,
                            RuntimeOptions.OBJECT_CACHE_SIZE_DEFAULT));
            
            final int minObjectCacheSize = 1; // allows some test cases but otherwise not reasonable.
            
            final int maxObjectCacheSize = 100000;
            
            if (objectCacheSize <= minObjectCacheSize) {
            
                throw new IllegalArgumentException(
                        RuntimeOptions.OBJECT_CACHE_SIZE + ": minmum value is "
                                + minObjectCacheSize);
                
            }
            if (objectCacheSize > maxObjectCacheSize) {
                
                throw new IllegalArgumentException(
                        RuntimeOptions.OBJECT_CACHE_SIZE
                                + ": maximum value is " + maxObjectCacheSize);
                
            }
            
            log.info(RuntimeOptions.OBJECT_CACHE_SIZE + "=" + objectCacheSize);
            
            cache = new WeakValueCache(new LRUCache(objectCacheSize));

            cache.setListener(new CacheListener());
            
        }

        // Create/open the backing store.
        m_journal = new Journal(properties);

        /*
         * register/open the main indices.
         */
        registerIndices();
        
        // FIXME Setup serializers.

//        ISerializationHandler ser = getRecordManager()
//                .getSerializationHandler();
//
//        if (ser instanceof ExtensibleSerializerSingleton) {
//
//            IExtensibleSerializer _ser = ((ExtensibleSerializerSingleton) ser)
//                    .getSerializer(getRecordManager());
//
//            om.setupSerializers(_ser);
//
//            registerSerializer(_ser);
//            
//        }

    }

    /**
     * This is used both during abort processing (to discard writes on the
     * indices) and when the object manager is created (to load/create the
     * various indices).
     */
    private void registerIndices() {

        oid_ndx = name_ndx = pcls_ndx = null;
        
        str_id_ndx = id_str_ndx = null;
        
        getOidIndex();
        
        getNameIndex();
        
        getPropertyClassIndex();
        
        getStrIdIndex();
        
        getIdStrIndex();

    }
    
    /**
     * Registers an index.
     * 
     * @param name
     *            The index name.
     * 
     * @todo update the test suites so that they can pass when the index
     *       supports isolation (entry counts are upper bounds rather than
     *       exact).
     */
    private BTree registerIndex(String name) {
        
        final int branchingFactor = m_journal.getDefaultBranchingFactor();
        
//        BTree ndx = new UnisolatedBTree(m_journal, branchingFactor, UUID.randomUUID());
        
        IndexMetadata metadata = new IndexMetadata(name,UUID.randomUUID());

        metadata.setBranchingFactor(branchingFactor);
        
        BTree ndx = BTree.create(m_journal, metadata); 
        
//        BTree ndx = new BTree(m_journal, branchingFactor, UUID.randomUUID(),
//                ByteArrayValueSerializer.INSTANCE);
        
        return (BTree) m_journal.registerIndex(name,ndx);
        
    }
    
    /**
     * The {@link BTree} in which persistent objects are stored.
     */
    private BTree getOidIndex() {

        oid_ndx = (BTree) m_journal.getIndex(OID_NDX);

        if (oid_ndx == null) {

            oid_ndx = registerIndex(OID_NDX);
            
        }
        
        return oid_ndx;
        
    }
    
    /**
     * The {@link BTree} for associating a persistent object with a Unicode
     * name.
     */
    private BTree getNameIndex() {

        name_ndx = (BTree) m_journal.getIndex(NAME_NDX);

        if (name_ndx == null) {

            name_ndx = registerIndex(NAME_NDX);

        }
        
        return name_ndx;
        
    }

    /**
     * Returns the {@link BTree} instance used by the object manager to map the
     * name of a property class onto the {@link Long oid} of that property
     * class.
     */
    private BTree getPropertyClassIndex() {

        pcls_ndx = (BTree) m_journal.getIndex(PCLS_NDX);

        if (pcls_ndx == null) {

            pcls_ndx = registerIndex(PCLS_NDX);

        }
        
        return pcls_ndx;
        
    }

    /**
     * Forward map for the string index.
     */
    private BTree getStrIdIndex() {

        str_id_ndx = (BTree) m_journal.getIndex(STR_ID_NDX);

        if (str_id_ndx == null) {

            str_id_ndx = registerIndex(STR_ID_NDX);

        }
        
        return str_id_ndx;
        
    }

    /**
     * Reverse map for the string index.
     */
    private BTree getIdStrIndex() {

        id_str_ndx = (BTree) m_journal.getIndex(ID_STR_NDX);

        if (id_str_ndx == null) {

            id_str_ndx = registerIndex(ID_STR_NDX);

        }
        
        return id_str_ndx;
        
    }

    /**
     * Registers serializers for integration classes.
     */
    protected void registerSerializer( IExtensibleSerializer ser )
    {

        ser.registerSerializer(MyBTree.class, MyBTree.Serializer0.class);
        
    }
    
    /**
     * Close the store immediately.
     */
    public void close() {

        m_journal.shutdown();

        m_journal = null; // clear the reference.

    }

    public void setNamedObject(final String name, long oid) {

        if (name == null) {

            throw new IllegalArgumentException();

        }

        final byte[] key = getKeyBuilder().reset().append(name).getKey();
        
        if(oid==0L) {

            name_ndx.remove(key);
            
        } else {
            
            final byte[] val = getKeyBuilder().reset().append(oid).getKey();

            name_ndx.insert(key, val);
            
        }

    }

    public long getNamedObject(String name) {

        if (name == null) {

            throw new IllegalArgumentException();

        }

        final byte[] key = getKeyBuilder().reset().append(name).getKey();
        
        final byte[] val = (byte[]) name_ndx.lookup(key);

        if (val == null) return 0L;

        long oid = KeyBuilder.decodeLong(val, 0);
        
        return oid;

    }
    
    /**
     * The GOM layer object cache.
     */
    final WeakValueCache cache;

    /**
     * Counters tracking various things of interest.
     */
    final private Counters _counters = new Counters();

    /**
     * A class containing counters tracking things of interest in the
     * integration with the bigdata database.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    class Counters {

//        int txBegin;
//
//        int txCommit;
//
//        int txAbort;

        int objectsInserted;

        int objectsFetched;

        int objectsFetchedFromCache;

        int objectsFetchedFromFD;

        int objectsUpdated;

        int objectsDeleted;

        int dirtyObjectsInstalledByCommit;

        int dirtyObjectsEvictedFromCache;

        int objectsEvictedFromCache;

        public Counters() {

            resetCounters();
            
        }

        public void resetCounters() {

//            txBegin = txCommit = txAbort = 0;

            objectsInserted = 0;
            objectsFetched = objectsFetchedFromCache = objectsFetchedFromFD = 0;
            objectsUpdated = 0;
            objectsDeleted = 0;

            dirtyObjectsInstalledByCommit = dirtyObjectsEvictedFromCache = objectsEvictedFromCache = 0;

        }

        /**
         * Counters only written when the {@link #log} level is DEBUG or less.
         */
        public void writeCounters() {

            if (!isDEBUG())
                return;

            log.info("---- PersistenceStore Counters ----");

//            log.info("tx: begin=" + txBegin + ", commit=" + txCommit
//                    + ", abort=" + txAbort);

            log.info("objects: inserted=" + objectsInserted + ", fetched (all="
                    + objectsFetched + ", cache=" + objectsFetchedFromCache
                    + ", federation=" + objectsFetchedFromFD + ")"
                    + ", updated=" + objectsUpdated + ", deleted="
                    + objectsDeleted);

            log.info("objectEvictedFromCache: all=" + objectsEvictedFromCache
                    + ", dirty=" + dirtyObjectsEvictedFromCache);

            log.info("objectsInstalledByCommit: "
                    + dirtyObjectsInstalledByCommit);

            resetCounters();

        }
    }

    /**
     * @todo cluster an object with the container by forming the oid of the
     *       object from the oid of the container (interpreted as a byte[]) and
     *       appending a within container unique integer (expressed as a
     *       byte[]).
     */
    public long insert(BaseObject container, BaseObject obj) {
        
        long oid = oid_ndx.getCounter().incrementAndGet();
        
        if (oid == 0L) {

            // Never assign 0L, which is the 1st value for the counter.
            
            oid = oid_ndx.getCounter().incrementAndGet();

        }

        /*
         * Insert the object into the object cache under the assigned object
         * identifier. The object is initially considered to be "dirty" so that
         * it will be eventually written through to the store if the transaction
         * commits.
         * 
         * Note: The object identifier field on the object is NOT valid yet so
         * we pass in the assigned object identifier. The ObjectManager will
         * take responsibility for setting the object identifier on the object
         * once this method returns.
         * 
         * Note: We do NOT insert the object into the object index until it has
         * been either evicted from the object cache or at the next commit.
         * Until then it is just live in memory.
         */

        cache.put(oid, obj, true);

        if (isDEBUG()) {

            log.debug("oid=" + oid + ", obj=" + obj);
            
        }

        _counters.objectsInserted++;
        
        return oid;

    }

    public void delete(long oid) {

        final byte[] key = getKeyBuilder().reset().append(oid).getKey();

        oid_ndx.remove(key);
        
        cache.remove(oid);

        if (isDEBUG()) {

            log.debug("oid=" + oid);

        }

        _counters.objectsDeleted++;
        
    }

    public void delete(BaseObject obj ) {

        final long oid = obj.getOID();
        
        delete(oid);

    }
    
    public BaseObject fetch(long oid) {

        // test the cache.
        BaseObject obj = (BaseObject) cache.get(oid);

        if (obj == null) {

            // read from the object index.
            
            final byte[] key = getKeyBuilder().reset().append(oid).getKey();

            final byte[] data = (byte[]) oid_ndx.lookup(key);

            if(data == null) {

                log.warn("No such object: oid=" + oid);

                return null;
                
            }
            
            // deserialize.
            
            obj = deserialize( data );
            
            /*
             * Insert into the cache.
             * 
             * Note: The ooOID has not been set on the object yet. This is done
             * as soon as we return from this call. We could do it here since we
             * have all the necessary information on hand, but the division of
             * labor currently says that the OM is responsible for this.
             */

            cache.put(oid, obj, false);

            _counters.objectsFetchedFromFD++;

        } else {

            _counters.objectsFetchedFromCache++;

        }

        _counters.objectsFetched++;
        
        return obj;

    }

    /**
     * Touch the object in the {@link WeakValueCache object cache}, marking it
     * as dirty. The object will be written onto the object index until either
     * the transaction {@link #commit() commits} or the object is evicted from
     * the object cache.
     * 
     * @todo Thread-safety: must synchronize on the object cache.
     */
    public void update(BaseObject obj) {

        final long oid = obj.getOID();
        
        if (isDEBUG() && cache.get(oid) == null) {
                
            /*
             * Note: This is an error since the cache should retain an entry for
             * an object until it is no longer weakly reachable. That means it
             * is not possible for the application to have retained a reference
             * for the object and hence impossible to have passed that reference
             * into this method.
             * 
             * Note: Generic( om ) uses update() to insert the object into the
             * cache. This should probably be revisited, but for now it means
             * that we need to insert the object into the cache if it was not
             * found.
             */

            log.error("No cache entry: " + obj);

            // throw new AssertionError("object not in cache.");
            
        }

        // mark as dirty.
        cache.put(oid, obj, true);
        
        // update counters.
        _counters.objectsUpdated++;
        
        if (isDEBUG()) {
        
            log.debug("oid=" + oid + ", obj=" + obj);
            
        }

    }

    private byte[] serialize(BaseObject obj) {
        
        return SerializerUtil.serialize(obj);
        
    }
    
    private BaseObject deserialize(byte[] data) {
        
        if(data==null) return null;

        BaseObject obj = (BaseObject) SerializerUtil.deserialize(data);
        
        if (isDEBUG()) {

            log.debug("deserialized: " + obj + " from " + data.length
                    + " bytes (" + Arrays.toString(data) + ")");

        }

        return obj;
        
    }
    
    /**
     * Uses eviction notices from the object cache to write dirty objects onto
     * the object index.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     */
    private final class CacheListener implements ICacheListener {

        public CacheListener() {
        
        }

        /**
         * @todo this should use an ordered write. Buffer the (possibly
         *       serialized) objects that are being evicted in 2nd hard
         *       reference cache and then periodically flush the objects in an
         *       ordered write to the index. in order to remain coherent all
         *       cache tests will also have to be performed against this 2nd
         *       hard reference cache and the objects will have to be
         *       re-inserted into the weak value cache if they are re-fetched
         *       before they have been flushed to the object index.
         */
        public void objectEvicted(ICacheEntry entry) {

            if (entry.isDirty()) {

                /*
                 * Note: We do NOT have to clear the dirty flag on the hard
                 * reference cache entry since the object is being evicted from
                 * the cache and the entry will be recycled for some incoming
                 * object with its own dirty flag state.
                 */

                BaseObject obj = (BaseObject) entry.getObject();
                
                final byte[] key = getKeyBuilder().reset().append(obj.getOID()).getKey();
                
                oid_ndx.insert(key, serialize(obj));

                _counters.dirtyObjectsEvictedFromCache++;

            }

            _counters.objectsEvictedFromCache++;

        }

    }

    public void commit() {

        // scan the cache looking for dirty objects.

        Iterator itr = cache.entryIterator();

        while (itr.hasNext()) {

            ICacheEntry entry = (ICacheEntry) itr.next();

            if (entry.isDirty()) {

                /*
                 * Force updates out to the btree.
                 * 
                 * @todo this should use an ordered write. Pull everything that
                 * is dirty into an array, sort it by object identifier, and
                 * then do an ordered write on the object index.
                 */
                
                BaseObject obj = (BaseObject) entry.getObject();

                // write on the object index.
                
                final byte[] key = getKeyBuilder().reset().append(obj.getOID()).getKey();

                oid_ndx.insert(key, serialize(obj));
                
                /*
                 * Clear the dirty flag. Unlike a cache eviction notice, the
                 * object will remain in the hard reference cache so we need to
                 * clear the dirty flag to indicate that it no longer needs to
                 * be installed on the federation.
                 */

                entry.setDirty(false);

                _counters.dirtyObjectsInstalledByCommit++;

            }

        }

        m_journal.commit();
        
        // write counters.
        _counters.writeCounters();

    }

    public void rollback() {

        // clear the object cache.
        
        cache.clear();

        // discard the write sets.
        
        m_journal.abort();
        
        /*
         * release references to the indices since they may have invalid data
         * and re-fetch the indices from the store.
         */
        registerIndices();
		
	}

    public int intern( String s ) {

        return intern( s, true );
        
    }

    /**
     * Conditionally interns <i>s </i> in the string table.
     * 
     * @param s
     *            Some string.
     * 
     * @param insert
     *            When <code>true</code> <i>s </i> is interned if is not
     *            already in the string table.
     * 
     * @return The stringId associated with <i>s </i>, <code>0</code> iff
     *         <code>insert == false</code> and <i>s </i> is not in the string
     *         table (zero is never a valid stringId).
     * 
     * @exception IllegalArgumentException
     *                if <i>s == null </i>.
     * 
     * @see #disintern( int stringId )
     */
    public int intern( final String s, boolean insert )
    {

        if( s == null ) {
            
            throw new IllegalArgumentException();
            
        }

        final byte[] key1 = getKeyBuilder().reset().append(s).getKey();
        
        final byte[] val = (byte[]) str_id_ndx.lookup( key1 );
        
        if( val != null) {
           
            long id = KeyBuilder.decodeLong(val, 0);

            if(id>Integer.MAX_VALUE) {

                // see comment below.
                
                throw new AssertionError();
                
            }
            
            return (int) id;
            
        }
        
        final long tmp = str_id_ndx.getCounter().incrementAndGet();
        
//        if(tmp==0L) {
//            
//            // skip over the counter value of zero.
//            
//            tmp = str_id_ndx.getCounter().inc();
//            
//        }
        
        if (tmp > Integer.MAX_VALUE) {

            /*
             * @todo in a scale-out index the counter will overflow since the
             * index partition identifier is in the high half of the long
             * counter value. track down uses of intern and decide whether or
             * not to keep this feature or to promote the return type to long.
             */

            throw new RuntimeException("String table is full");

        }

        {

            final byte[] key2 = getKeyBuilder().reset().append((long)tmp).getKey();

            str_id_ndx.insert(key1, key2);

            id_str_ndx.insert(key2, SerializerUtil.serialize(s));

        }

        return (int) tmp;
        
    }
    
    public String disintern( int stringId )
    {

        final byte[] key = getKeyBuilder().reset().append((long)stringId).getKey();

        final byte[] val = (byte[]) id_str_ndx.lookup(key);
        
        if (val == null) {

            throw new IllegalArgumentException("" + stringId);
            
        }
       
        return (String) SerializerUtil.deserialize(val);
        
    }
    
    //
    // Blob factory.
    //
    
    public IBlob newBlob()
    {
        
        return new Blob( om );
        
    }
    
    public void removeBlob(IBlob blob)
    {
        
        ((Blob)blob).remove();
        
    }

    //
    // PropertyClass support.
    //

    public IPropertyClass getPropertyClass(final String property, boolean insert) {

        if(property==null) throw new IllegalArgumentException();
        
        final PropertyClass propertyClass;

        // Lookup the recid of the named property class.

        final byte[] key = getKeyBuilder().reset().append(property).getKey();
        
        byte[] val = (byte[]) pcls_ndx.lookup(key);

        if (val == null) {

            /*
             * [recid] is null iff this property was never registered with the
             * object manager.
             */

            if (!insert) {

                // If not inserting, then we are done.

                return null;

            }

            /*
             * Create a new property class.
             */

            propertyClass = new PropertyClass(om, property);

            // // Set the type code on the property class object.
            //
            // propertyClass.setTypeCode
            // ( TypeCode.PROPERTY_CLASS
            // );

            // Insert into the btree so that we can find the same
            // object again.

            final Long oid = new Long(propertyClass.getOID());
            
            final Object oldValue = pcls_ndx.insert(key, KeyBuilder.asSortKey(oid));

            if (oldValue != null) {

                throw new RuntimeException("Index already contains property="
                        + property);

            }

            log.debug("New propertyClass: " + property);

            // Return the property class.

            return propertyClass;

        } else {

            /*
             * Fetch the pre-existing property class.
             */

            long oid = KeyBuilder.decodeLong(val, 0);
            
            return om.getPropertyClass(oid);

        }

    }

    /**
     * <p>
     * Creates and returns a btree configured for the specified
     * {@link LinkSetIndex}.
     * </p>
     * <p>
     * We choose {@link Comparator}, {@link Coercer} and {@link Successor}
     * implementations based on the property type constraint (if any) associated
     * with the value propertyClass for the link set index. These objects MUST
     * be choosen with a consistent set of assumptions in mind in order to
     * produce an index that (a) works and (b) imposes a natural ordering on the
     * link set members. If there is a type constraint on the key then we always
     * use a NOP coercer. If there is no type constraint, then generic type
     * conversion rules are applied by {@link DefaultUnicodeCoercer}.
     * </p>
     * <p>
     * Note: The bigdata integration only supports the case where the attribute
     * is part of the key - it does not allow the dynamic resolution of the
     * attribute value from the object identifier (storeOidOnly must be false).
     * Indirection through the object identifier is an enormous performance
     * penalty. Further bigdata requires that all keys are unsigned byte[]s and
     * does not allow the use of custom comparators. In order to achieve the
     * same effect, you should use a computed property as the attribute on which
     * the index is ordered. For example, you can perform case-folding or
     * reorder the components of a URI in the computed property.
     * </p>
     * <p>
     * Note: All keys for bigdata indices are encoded as unsigned byte[]s.
     * However {@link KeyBuilder#asSortKey(Object)} transparently converts
     * application keys into unsigned byte[]s. Therefore the coercer is only
     * used when the property value is not strongly typed and we need to apply
     * generic value conversion. See {@link KeyBuilder} for forming keys for a
     * variety of purposes.
     * </p>
     * <p>
     * When duplicate keys are allowed in the link set index the composite key
     * is an unsigned byte[] formed by appending the object identifier to the
     * coerced key. How this is done depends on whether or not the coerced key
     * is a String. String are handled using
     * {@link IKeyBuilder#appendText(String, boolean, boolean)}. See
     * {@link MyBTree#newCompositeKey(Object, long)}.
     * </p>
     * 
     * @todo Other kinds of arrays of Java primitives: int[], long[], etc. are
     *       not supported at this time.
     * 
     * @see LinkSetIndex#getValuePropertyClass()
     * @see PropertyClass#getType()
     * @see BTree
     * @see IKeyBuilder
     * 
     * @throws UnsupportedOperationException
     *             if the type constraint on the value property is not
     *             supported. In particular {@link Character} is NOT supported
     *             since its semantics are ambiguous (Unicode vs signed
     *             integer). Instead use String for Unicode or Short for signed
     *             2 byte integers.
     * @throws UnsupportedOperationException
     *             if the value property specifies a custom comparator - use a
     *             computed attribute instead.
     * @throws UnsupportedOperationException
     *             if {@link LinkSetIndex#getStoreOidOnly()} is
     *             <code>true</code>.
     */
    synchronized public AbstractBTree createBTree(final LinkSetIndex ndx) {

        if(ndx.getStoreOidOnly()) {
            
            throw new UnsupportedOperationException("storeOidOnly");
            
        }
        
        final PropertyClass valuePropertyClass = (PropertyClass) ndx
                .getValuePropertyClass();

        final Successor successor;

        /*
         * The values are always object identifiers.
         */
//        final IValueSerializer valueSerializer = OidSerializer.INSTANCE;
        
        /*
         * The type constraint associated with the property value (may be null).
         */
        final Class type = valuePropertyClass.getType();

        /*
         * How an attribute is coerced depends on the type constraint associated
         * with the property value (if any). In all cases the attribute value is
         * coerced to an unsigned byte[].
         */
        final Coercer coercer;
        
        if (type == null) {

            /*
             * The key class is unconstrained. Different keys instances may have
             * different types. Generic value type conversion rules are applied
             * to coerce the value to a {@link String}.
             */
            
            coercer = new DefaultUnicodeCoercer(); // coerce to String.
            
//            successor = new StringSuccessor();
            
            /*
             * FIXME We need to use special successor semantics both for generic
             * type conversion and for indices that are explicitly typed as
             * having String attributes. The correct successor semantics are
             * available using IKeyBuilder#appendText(...) and specifying
             * successor := true.
             */
            successor = null;

        } else if (type == Byte.class   || type == Character.class
                || type == Short.class  || type == Integer.class
                || type == Long.class   || type == Float.class
                || type == Double.class || type == String.class) {
        
            /*
             * Simple conversion rules are applied for all of these cases.
             */
            
            if (type == Character.class) {
             
                /*
                 * Note: Characters are ordered as signed short integers (eg.,
                 * according to their code points rather than to their
                 * interpretation by a Unicode collator).
                 */
                throw new UnsupportedOperationException(
                        "Character is not allowed as a type restriction.  Use String for Unicode semantics or Short for a signed two byte integer.");

            } else {

                coercer = null;
                
            }
            
            if (type == Byte.class) {
                
                successor = new ByteSuccessor();
                
            } else if (type == Character.class) {
                
                successor = new CharacterSuccessor();
                
            } else if (type == Short.class) {
                
                successor = new ShortSuccessor();
                
            } else if (type == Integer.class) {
                
                successor = new IntegerSuccessor();
                
            } else if (type == Long.class) {
                
                successor = new LongSuccessor();
                
            } else if (type == Float.class) {
                
                successor = new FloatSuccessor();
                
            } else if (type == Double.class) {
                
                successor = new DoubleSuccessor();
                
            } else if (type == String.class) {
                
//                successor = new StringSuccessor();
                successor = null;
                
            } else {
                
                throw new AssertionError();
                
            }

        } else if (type == Array.class && type.getComponentType() == Byte.class) {

            /*
             * Special case for byte[] keys.
             */
            
            coercer = null; // no coercion required.
           
            successor = UnsignedByteArraySuccessor.INSTANCE;
            
        } else {
            
            /*
             * Otherwise the type constraint is not handled by the index.
             */
            
            throw new UnsupportedOperationException(
                    "Unsupported property type constraint: type="
                            + type.getName());

        }

        if(valuePropertyClass.getPropertyValueComparator() != null) {

            /*
             * Note: Explicit comparators are not supported. You need to
             * generate a computed attribute that imposes the designed ordering.
             */
            
            throw new UnsupportedOperationException(
                    "Custom comparators are not supported - use a computed attribute instead: "
                            + valuePropertyClass.getProperty());
            
        }
        
        if (true) {

            log.debug("keyType=" + (type == null ? "N/A" : type.getName()));
            log.debug("duplicateKeys=" + ndx.getDuplicateKeys());
            log.debug("coercer="
                    + (coercer == null ? "N/A" : coercer.getClass().getName()));
            log.debug("successor=" + (successor==null?"N/A":successor.getClass().getName()));

        }

        /*
         * Note: we are not registering the btree as a named index at this 
         * time.  That might be changed later - see the javadoc above and
         * on MyBTree for notes on the issues here. 
         */
        
//        String name = ndx.getLinkPropertyClass().identity();

        final IndexMetadata metadata = new IndexMetadata(UUID.randomUUID());
        
        metadata.setBranchingFactor(m_journal.getDefaultBranchingFactor());
        
        final BTree btree;
//        btree = (BTree) m_journal.getIndex(name);
        
//        if(btree==null) {

        btree = BTree.create(m_journal, metadata);
        
//            btree = new BTree(m_journal, m_journal.getDefaultBranchingFactor(),
//                    UUID.randomUUID(), valueSerializer);
            
            // force the btree to the store so that we can get its checkpoint record.
            btree.writeCheckpoint();

//            m_journal.registerIndex(name, btree);
            
//        }

        return new MyBTree(ndx, btree, coercer, successor, null/*comparator*/);

    }

    synchronized public AbstractBTree getBTree( long oid ) {

        return (MyBTree) om.fetch(oid);
        
    }

//    /**
//     * The value is a <code>long</code> integer that is the term identifier.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
//     *         Thompson</a>
//     * @version $Id$
//     */
//    static private class OidSerializer implements IValueSerializer {
//
//        private static final long serialVersionUID = 1815999481920227061L;
//
//        public static transient final IValueSerializer INSTANCE = new OidSerializer();
//        
//        final static boolean packedLongs = true;
//        
//        public OidSerializer() {}
//        
//        public void getValues(DataInput is, Object[] values, int n)
//                throws IOException {
//
//            for(int i=0; i<n; i++) {
//                
//                if (packedLongs) {
//
//                    values[i] = Long.valueOf(LongPacker.unpackLong(is));
//
//                } else {
//
//                    values[i] = Long.valueOf(is.readLong());
//
//                }
//                
//            }
//            
//        }
//
//        public void putValues(DataOutputBuffer os, Object[] values, int n)
//                throws IOException {
//
//            for(int i=0; i<n; i++) {
//
//                if(packedLongs) {
//
////                    LongPacker.packLong(os, ((Long) values[i]).longValue());
//                    os.packLong(((Long) values[i]).longValue());
//                    
//                } else {
//
//                    os.writeLong(((Long) values[i]).longValue());
//                
//                }
//                
//            }
//            
//        }
//        
//    }
    
}
