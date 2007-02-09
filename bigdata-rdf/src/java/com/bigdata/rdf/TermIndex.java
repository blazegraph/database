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
package com.bigdata.rdf;


import org.openrdf.model.Value;

import com.bigdata.journal.Journal;
import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.UserDefinedFunction;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rdf.rio.BulkRioLoader;
import com.bigdata.rdf.serializers.TermIdSerializer;

/**
 * A persistent index mapping variable length byte[] keys formed from an RDF
 * {@link Value} to {@link Long} integer term identifiers.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TermIndex extends BTree {

    public final AutoIncCounter counter;
    
    /**
     * Auto-increment counter.
     * <p>
     * 
     * Note: In order to be safe for distributed indices concurrent writes on
     * the terms index on different journals for different partitions of the
     * terms index MUST NOT assign the same term identifier. This can be
     * achieved by managing the {@link #indexId} field such that it is really a
     * {@link Journal} identifier.
     * <p>
     * Note: changing this to a UUID would have the effect of scattering the
     * term identifiers randomly across index partitions and I am not sure how
     * that would effect index performance when mapping terms to identifiers. It
     * could help if we scatter queries and it could hurt by touching more or
     * all term index partitions on each query. Changing to a UUID would also
     * change the datatype from long to UUID and there is a lot of code that
     * currently assumes a <code>long</code>. Finally, UUIDs appear to have
     * fat serialization and are declared as <code>final</code>.
     * 
     * @todo the {@link BulkRioLoader} also uses this counter and that use needs
     *       to be reconciled for consistency if concurrent writers are allowed.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AutoIncCounter implements UserDefinedFunction {

        /**
         * 
         */
        private static final long serialVersionUID = -4281749674236461781L;

        /**
         * The next identifier to be assigned to a string inserted into this
         * index.
         * 
         * @todo this needs to be (a) shared across all transactional instances
         *       of this index; (b) set into a namespace that is unique to the
         *       journal so that multiple writers on multiple journals for a
         *       single distributed database can not collide; and (c) set into a
         *       namespace that is unique to the index.
         */
        long nextId;
        
        /**
         * An int16 value that may be used to multiplex identifier assignments for
         * one or more distributed indices. When non-zero the next identifier that
         * would be assigned by this index is first shifted 16 bits to the left and
         * then ORed with the indexId. This limits the #of distinct terms that can
         * be stored in the index to 2^48.
         * 
         * @todo for a paritioned index this needs to get into place based on the
         *       journal to which the request is directed.
         */
        final short indexId;
        
        /**
         * The last value assigned by the counter.
         */
        private Object retval;
        
        public AutoIncCounter(short indexId,long nextId) {

            assert nextId > 0;
            
            this.indexId = indexId;
            
            this.nextId = nextId;
            
        }
        
        public long nextId() {

            long id = nextId++;
            
            if( indexId != 0 ) {
                
                id <<= 16;
                
                id |= indexId;
                
            }
            
            return id;

        }
        
        /**
         * If the key is found then we do not update the value.
         */
        public Object found(byte[] key, Object val) {

            this.retval = val;
            
            return val;
            
        }

        /**
         * If the key is not found then we insert the current value of the
         * counter and increment the counter.
         */
        public Object notFound(byte[] key) {
            
            retval = Long.valueOf(nextId());
            
            return retval;
            
        }
        
        public Object returnValue(byte[] key,Object oldval) {
            
            return retval;
            
        }
        
    }
    
    /**
     * Create a new index.
     * 
     * @param store
     *            The backing store.
     * 
     * @param indexId
     *            An int16 value that may be used to multiplex identifier
     *            assignments for one or more distributed indices. When
     *            non-zero the next identifier that would be assigned by
     *            this index is first shifted 16 bits to the left and then
     *            ORed with the indexId. This limits the #of distinct terms
     *            that can be stored in the index to 2^48.
     */
    public TermIndex(IRawStore store, short indexId) {

        super(store, DEFAULT_BRANCHING_FACTOR, TermIdSerializer.INSTANCE);

        counter = new AutoIncCounter( indexId, 1L );

//        System.err.println("TermIndexMetadata(create): indexId="+counter.indexId+", nextId="+counter.nextId);

    }
    
    /**
     * Load an index from the store.
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record identifier for the index.
     */
    public TermIndex(IRawStore store, long metadataId) {
    
        super(store, BTreeMetadata.read(store, metadataId));
        
        TermIndexMetadata md = (TermIndexMetadata)metadata;
        
//        System.err.println("TermIndexMetadata(reload): indexId=" + md.indexId
//                + ", nextId=" + md.nextId);

        counter = new AutoIncCounter(md.indexId, md.nextId);

    }

    protected BTreeMetadata newMetadata() {
        
        System.err.println("Creating TermIndexMetadata record.");
        
        return new TermIndexMetadata(this);
        
    }

    /**
     * Extends the metadata record to persist the state of the
     * {@link AutoIncCounter}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class TermIndexMetadata extends BTreeMetadata {

        private static final long serialVersionUID = -4489385552740076106L;

        final protected short indexId;
        final protected long nextId;

//        /**
//         * De-serialization constructor.
//         */
//        public TermIndexMetadata() {
//            
//        }
        
        protected TermIndexMetadata(TermIndex btree) {
            
            super(btree);
            
            this.indexId = btree.counter.indexId;
            this.nextId = btree.counter.nextId;
            
//            System.err.println("TermIndexMetadata(ctor): indexId="+indexId+", nextId="+nextId);
            
        }
        
    }
    
    /**
     * Lookup the term in the term:id index (non-batch api). if it is there then
     * take its termId. Otherwise, insert the term into the term:id index which
     * gives us its termId.
     * 
     * @param key
     *            The sort key for the term.
     * 
     * @return The termId, which was either assigned or resolved by the index.
     */
    public long add(byte[] key) {

        return (Long)insert(key,counter);
        
//        Long id = (Long)lookup(key);
//        
//        if( id == null ) {
//
//            id = nextId();
//            
//            insert(key,id);
//            
//        }
//        
//        return id;
        
    }

    /**
     * Get the existing term identifier.
     * 
     * @param key
     *            The term key.
     * 
     * @return The term identifier -or- 0L if the term was not found.
     */
    public long get(byte[] key) {
        
        Long id = (Long)lookup(key);
        
        if( id == null ) return 0L;
            
        return id;
        
    }
    
}
