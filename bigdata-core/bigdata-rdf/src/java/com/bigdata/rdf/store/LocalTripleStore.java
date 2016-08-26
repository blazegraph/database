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
 * Created on Jan 3, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.locator.DefaultResourceLocator;

/**
 * A triple store based on the <em>bigdata</em> architecture. This class
 * offers extremely low latency for index operations. All indices are local
 * (in-process) objects and there are no concurrency controls, so point tests on
 * the indices are extremely efficient. Significant parallelism is achieved by
 * paying careful attention to the concurrency constraints imposed by the
 * {@link BTree} class (writers are single threaded, reads may be concurrent,
 * but not concurrent with a writer) and by using different views (unisolated vs
 * read-historical) of the indices when computing entailments or performing
 * high-level query.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class LocalTripleStore extends AbstractLocalTripleStore {

    final static private Logger log = Logger.getLogger(LocalTripleStore.class);

    private final IJournal store;

    /**
     * The backing embedded database.
     */
    @Override
    public IJournal getIndexManager() {
        
        return store;
        
    }
    
    /**
     * Delegates the operation to the backing store.
     */
    @Override
    synchronized public long commit() {
     
      final long begin = System.currentTimeMillis();

      super.commit();

      final IIndexManager indexManager = getIndexManager();

      if (indexManager.isGroupCommit()) {
         /*
          * Note: GROUP_COMMIT (#566) requires that the WriteExecutorService
          * controls when commit points are melded. If group commit is enabled
          * and the index manager is directs the Journal to commit() then any
          * partially executed tasks that have checkpointed indices will be
          * immediately flushed to the backing store, breaking the ACID
          * semantics of the commit.
          * 
          * Note: The checkpoint of the indices for an unisolated AbstractTask
          * occurs when the task is done with its work but is still holding any
          * resource locks.
          */
         return 0L;
      }

      final long commitTime = getIndexManager().commit();

      final long elapsed = System.currentTimeMillis() - begin;

      if (log.isInfoEnabled())
         log.info("commit: commit latency=" + elapsed + "ms");

      return commitTime;

    }

    @Override
    synchronized public void abort() {
                
        super.abort();
        
        // discard the write sets.
        getIndexManager().abort();

    }
    
    @Override
    public boolean isStable() {
        
        return store.isStable();
        
    }
    
    @Override
    public boolean isReadOnly() {
        
        return super.isReadOnly() || store.isReadOnly();
        
    }

    @Override
    public void close() {
        
        super.close();
        
        if(!isReadOnly()) {

            store.shutdown();
            
        }
        
    }
    
//    public void __tearDownUnitTest() {
//        
//        super.__tearDownUnitTest();
//
//        if(!isReadOnly()) {
//
//            store.destroy();
//            
//        }
//        
//    }

    /**
     * Options understood by the {@link LocalTripleStore}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    public static interface Options extends AbstractTripleStore.Options {
       
    }
    
    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @param indexManager (must be an {@link IJournal}).
     * @param namespace
     * @param timestamp
     * @param properties
     */
    public LocalTripleStore(final IIndexManager indexManager,
            final String namespace, final Long timestamp,
            final Properties properties) {

        super(indexManager, namespace, timestamp, properties);

        if (indexManager instanceof IJournal) {

        store = (IJournal) indexManager;

        } else {

            /*
             * Hitting this code path may indicate that a TemporaryStore was
             * added to the DefaultResourceLocator as a "seeAlso" store to be
             * checked and a TempTripleStore was created on that TemporaryStore
             * under the same namespace as the LocalTripleStore. If the
             * LocalTripleStore is then destroyed or an abort() otherwise clears
             * it from the DefaultResourceLocator cache, then the
             * TempTripleStore having the same namespace can be discovered on
             * the TemporaryStore. This will cause an attempt to instantiate the
             * LocalTripleStore on the TemporaryStore, which will fail here.
             */
            throw new UnsupportedOperationException("Not an IJournal: indexManager=" + indexManager + ", namespace="
                    + namespace + ", timestamp=" + TimestampUtility.toString(timestamp) + ", properties=" + properties);
            
        }
        
    }

    /**
     * Create or re-open a triple store using a local embedded database.
     * <p>
     * Note: This is only used by the test suites.
     */
	/* public */static LocalTripleStore getInstance(final Properties properties) {

	    final String namespace = "kb";
	    
	    // create/re-open journal.
	    final Journal journal = new Journal(properties);
	    
        try {
            
          // Check for pre-existing instance.
          {

              final LocalTripleStore lts = (LocalTripleStore) journal
                      .getResourceLocator().locate(namespace, ITx.UNISOLATED);

              if (lts != null) {

                  return lts;

              }

          }
          
          // Create a new instance.
          {
          
              final LocalTripleStore lts = new LocalTripleStore(
                      journal, namespace, ITx.UNISOLATED, properties);
              
//              if (Boolean.parseBoolean(properties.getProperty(
//                      BigdataSail.Options.ISOLATABLE_INDICES,
//                      BigdataSail.Options.DEFAULT_ISOLATABLE_INDICES))) {
//              
//                  final long txCreate = txService.newTx(ITx.UNISOLATED);
//      
//                  final AbstractTripleStore txCreateView = new LocalTripleStore(
//                          journal, namespace, Long.valueOf(txCreate), properties);
//      
//                  // create the kb instance within the tx.
//                  txCreateView.create();
//      
//                  // commit the tx.
//                  txService.commit(txCreate);
//                  
//              } else {
                  
                  lts.create();
                  
//              }
              
          }

            /*
             * Now that we have created the instance locate the triple store
             * resource and return it.
             */
          {

              final LocalTripleStore lts = (LocalTripleStore) journal
                      .getResourceLocator().locate(namespace, ITx.UNISOLATED);

              if (lts == null) {

                  /*
                   * This should only occur if there is a concurrent destroy,
                   * which is highly unlikely to say the least.
                   */
                  throw new RuntimeException("Concurrent create/destroy: "
                          + namespace);

              }

              return lts;
              
          }
          
      } catch (Throwable ex) {
          
          journal.shutdownNow();
          
          throw new RuntimeException(ex);
          
      }

//      /*
//         * FIXME This should pass up the existing properties for the KB instance
//         * when the KB instance is pre-existing.  Really though, you should first
//         * obtain the Journal and then attempt to locate the KB and create it if
//         * it does not exist.
//         */
//        this(new Journal(properties), "kb"/* namespace */, ITx.UNISOLATED,
//                properties);
//        
//        /*
//         * FIXME Modify this to use a row scan for the contained relations.
//         * There is one other place where the same test is being used. The
//         * reason for this test is that getSPORelation() tries to _locate_ the
//         * relation, but that will fail if it does not exist. By using the ctor
//         * and exists() we can test for pre-existence. However, the best route
//         * is to perform a row scan when the container is created and then we
//         * can just materialize the existing relations and create them if they
//         * are not found.
//         */
//        if (!new SPORelation(getIndexManager(), getNamespace() + "."
//                + SPORelation.NAME_SPO_RELATION, getTimestamp(), getProperties()).exists()) {
//
//            /*
//             * If we could not find the SPO relation then presume that this is a
//             * new KB and create it now.
//             */
//            
//            create();
//
//        } else {
//        	
//        	init();
//        	
//        }

    }

    /**
     * When using an {@link ITx#UNISOLATED} view, this store is NOT safe for
     * write operations concurrent with either readers or writers. However, it
     * does support concurrent readers for {@link ITx#READ_COMMITTED} and
     * read-historical views.
     */
    @Override
    public boolean isConcurrent() {

        return getTimestamp() == ITx.UNISOLATED;
        
    }

}
