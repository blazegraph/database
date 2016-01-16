/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 3, 2010
 */

package com.bigdata.rdf.lexicon;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;

import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexTypeEnum;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.search.FullTextIndex;
import com.bigdata.search.Hit;
import com.bigdata.search.TokenBuffer;

/**
 * Implementation based on the built-in keyword search capabilities for bigdata.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BigdataRDFFullTextIndex.java 4709 2011-06-15 16:23:22Z thompsonbry $
 */
public class BigdataSubjectCentricFullTextIndex extends FullTextIndex implements
        ISubjectCentricTextIndexer<Hit> {

    final private static transient Logger log = Logger
            .getLogger(BigdataSubjectCentricFullTextIndex.class);
    
    /**
     * The basename of the search index.
     */
    public static final transient String NAME_SUBJ_SEARCH = "subjectSearch";

    static public BigdataSubjectCentricFullTextIndex getInstance(
            final IIndexManager indexManager, final String namespace,
            final Long timestamp, final Properties properties) {

        if (namespace == null)
            throw new IllegalArgumentException();

        return new BigdataSubjectCentricFullTextIndex(indexManager, namespace, timestamp,
                properties);

    }

    /**
     * When true, index datatype literals as well.
     */
    private final boolean indexDatatypeLiterals;
    
    public boolean getIndexDatatypeLiterals() {
        
        return indexDatatypeLiterals;
        
    }
    
    /**
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     */
    public BigdataSubjectCentricFullTextIndex(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        /*
         * Also index datatype literals?
         */
        indexDatatypeLiterals = Boolean
                .parseBoolean(getProperty(
                        AbstractTripleStore.Options.TEXT_INDEX_DATATYPE_LITERALS,
                        AbstractTripleStore.Options.DEFAULT_TEXT_INDEX_DATATYPE_LITERALS));

    }

    /**
     * Conditionally registers the necessary index(s).
     * 
     * @throws IllegalStateException
     *             if the client does not have write access.
     * 
     * @todo this is not using {@link #acquireExclusiveLock()} since I generally
     *       allocate the text index inside of another relation and
     *       {@link #acquireExclusiveLock()} is not reentrant for zookeeper.
     */
    @Override
    public void create() {

        assertWritable();
        
        final String name = getNamespace() + "."+NAME_SUBJ_SEARCH;

        final IIndexManager indexManager = getIndexManager();

//        final IResourceLock resourceLock = acquireExclusiveLock();
//
//        try {

            /*
             * Register a tuple serializer that knows how to unpack the values and
             * how to extract the bytes corresponding to the encoded text (they can
             * not be decoded) from key and how to extract the document and field
             * identifiers from the key.
             */
            final Properties p = getProperties();
            
            final IndexMetadata indexMetadata = new IndexMetadata(indexManager,
                    p, name, UUID.randomUUID(), IndexTypeEnum.BTree);

            /*
             * Override the collator strength property to use the configured
             * value or the default for the text indexer rather than the
             * standard default. This is done because you typically want to
             * recognize only Primary differences for text search while you
             * often want to recognize more differences when generating keys for
             * a B+Tree.
             * 
             * Note: The choice of the language and country for the collator
             * should not matter much for this purpose since the total ordering
             * is not used except to scan all entries for a given term, so the
             * relative ordering between terms does not matter.
             */
            final IKeyBuilderFactory keyBuilderFactory;
            {
            
                final Properties tmp = new Properties(p);

                tmp.setProperty(KeyBuilder.Options.STRENGTH, p.getProperty(
                    Options.INDEXER_COLLATOR_STRENGTH,
                    Options.DEFAULT_INDEXER_COLLATOR_STRENGTH));

                keyBuilderFactory = new DefaultKeyBuilderFactory(tmp);

            }

            final boolean fieldsEnabled = Boolean.parseBoolean(p
                    .getProperty(Options.FIELDS_ENABLED,
                            Options.DEFAULT_FIELDS_ENABLED));
    
            if (log.isInfoEnabled())
                log.info(Options.FIELDS_ENABLED + "=" + fieldsEnabled);
    
//            final boolean doublePrecision = Boolean.parseBoolean(p
//                    .getProperty(Options.DOUBLE_PRECISION,
//                            Options.DEFAULT_DOUBLE_PRECISION));
//    
//            if (log.isInfoEnabled())
//                log.info(Options.DOUBLE_PRECISION + "=" + doublePrecision);

            /*
             * FIXME Optimize. SimpleRabaCoder will be faster, but can do better
             * with record aware coder.
             */
            indexMetadata.setTupleSerializer(new RDFFullTextIndexTupleSerializer(
                    keyBuilderFactory,//
                    DefaultTupleSerializer.getDefaultLeafKeysCoder(),//
//                    DefaultTupleSerializer.getDefaultValuesCoder(),//
                    SimpleRabaCoder.INSTANCE,
                    fieldsEnabled
            ));
            
            indexManager.registerIndex(indexMetadata);

            if (log.isInfoEnabled())
                log.info("Registered new subject-centric text index: name=" + name);

            /*
             * Note: defer resolution of the index.
             */
//            ndx = getIndex(name);

//        } finally {
//
//            unlock(resourceLock);
//
//        }
        
    }
    
    /**
     * The full text index is currently located in the same namespace as the
     * lexicon relation.  However, the distributed zookeeper locks (ZLocks)
     * are not reentrant.  Therefore this method is overridden to NOT acquire
     * the ZLock for the namespace of the relation when destroying the full
     * text index -- that lock is already held for the same namespace by the
     * {@link LexiconRelation}.
     */
    @Override
    public void destroy() {

        if (log.isInfoEnabled())
            log.info("");
    
        assertWritable();
        
        final String name = getNamespace() + "." + NAME_SUBJ_SEARCH;
        
        getIndexManager().dropIndex(name);

    }
    
    /**
     * The backing index.
     */
    volatile private IIndex ndx;

    /**
     * The index used to associate term identifiers with tokens parsed from
     * documents.
     */
    public IIndex getIndex() {
        
        if(ndx == null) {

            synchronized (this) {

                ndx = getIndex(getNamespace() + "." + NAME_SUBJ_SEARCH);

                if (ndx == null)
                    throw new IllegalStateException();
                
            }
            
        }
        
        return ndx;
        
    }

    public void index(final IV<?,?> subject,
            final Iterator<BigdataValue> valuesIterator) {
        
    	if (subject == null) {
    		throw new IllegalArgumentException();
    	}
    	
    	if (log.isDebugEnabled()) {
    		log.debug("indexing: " + subject);
    	}
    	
    	/*
    	 * We can use a capacity of one, because we will be indexing exactly
    	 * one subject.
    	 */
        final TokenBuffer<?> buffer = new TokenBuffer(1, this);

        int n = 0;
        
        while (valuesIterator.hasNext()) {

            final BigdataValue val = valuesIterator.next();

        	if (log.isDebugEnabled()) {
        		log.debug("value: " + val);
        	}
        	
            if (!(val instanceof Literal)) {

                /*
                 * Note: If you allow URIs to be indexed then the code which is
                 * responsible for free text search for quads must impose a
                 * filter on the subject and predicate positions to ensure that
                 * free text search can not be used to materialize literals or
                 * URIs from other graphs. This matters when the named graphs
                 * are used as an ACL mechanism. This would also be an issue if
                 * literals were allowed into the subject position.
                 */
                continue;

            }

            final Literal lit = (Literal) val;

            if (!indexDatatypeLiterals && lit.getDatatype() != null) {

                // do not index datatype literals in this manner.
                continue;

            }

            final String languageCode = lit.getLanguage();

            // Note: May be null (we will index plain literals).
            // if(languageCode==null) continue;

            final String text = lit.getLabel();

            /*
             * Note: The OVERWRITE option is turned off to avoid some of the
             * cost of re-indexing each time we see a term.
             */

//            // don't bother text indexing inline values for now
//            if (termId.isInline()) {
//                continue;
//            }
            
            index(buffer, subject, 0/* fieldId */, languageCode,
                    new StringReader(text));

            n++;

        }
        
        // flush writes to the text index.
        buffer.flush();

        if (log.isInfoEnabled())
            log.info("indexed " + n + " new values for s: " + subject);

    }
    
}
