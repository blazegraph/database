/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Jul 25, 2007
 */

package com.bigdata.service;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.repo.BigdataRepository;
import com.bigdata.repo.BigdataRepository.Options;
import com.bigdata.resources.ResourceManager;
import com.bigdata.search.FullTextIndex;

/**
 * An abstract test harness that sets up (and tears down) the metadata and data
 * services required for a bigdata federation using in-process services rather
 * than service discovery (which means that there is no network IO).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME modify this and {@link AbstractLocalDataServiceFederationTestCase} to
 * be proxy test suites so that I can run tests against both with ease and make
 * use of them for testing the {@link FullTextIndex} and
 * {@link BigdataRepository}.
 */
abstract public class AbstractEmbeddedFederationTestCase extends AbstractBTreeTestCase {

    /**
     * 
     */
    public AbstractEmbeddedFederationTestCase() {
        super();
    }

    /**
     * @param arg0
     */
    public AbstractEmbeddedFederationTestCase(String arg0) {
        super(arg0);
    }

    protected IBigdataClient client;
    protected IBigdataFederation fed;
    protected IMetadataService metadataService;
    protected IDataService dataService0;
    protected IDataService dataService1;

    public Properties getProperties() {
        
        Properties properties = new Properties( super.getProperties() );
        
        // Note: uses transient mode for tests.
        properties.setProperty(Options.BUFFER_MODE, BufferMode.Transient
                .toString());
        
        // when the data are persistent use the test to name the data directory.
        properties.setProperty(EmbeddedClient.Options.DATA_DIR, getName());
        
        /*
         * Disable the o/s specific statistics collection for the test run.
         * 
         * Note: You only need to enable this if you are trying to track the
         * statistics or if you are testing index partition moves, since moves
         * rely on the per-host counters collected from the o/s.
         */
        properties.setProperty(EmbeddedClient.Options.COLLECT_PLATFORM_STATISTICS,"false");

        // disable moves.
        properties.setProperty(ResourceManager.Options.MAXIMUM_MOVES_PER_TARGET,"0");
        
        return properties;
        
    }

    private File dataDir;
    
    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp() throws Exception {
      
        dataDir = new File( getName() );
        
        if(dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete( dataDir );
            
        }

        client = new EmbeddedClient(getProperties());
        
        fed = client.connect();

        metadataService = fed.getMetadataService();
        System.err.println("metadataService: "+metadataService.getServiceUUID());

        dataService0 = ((EmbeddedFederation)fed).getDataService(0);
        System.err.println("dataService0   : "+dataService0.getServiceUUID());

        if (((EmbeddedFederation) fed).getDataServiceCount() > 1) {
        
            dataService1 = ((EmbeddedFederation)fed).getDataService(1);
            System.err.println("dataService1   : "+dataService1.getServiceUUID());
            
        }
        
    }
    
    public void tearDown() throws Exception {
        
        client.disconnect(true/*immediateShutdown*/);

        /*
         * Optional cleanup after the test runs, but sometimes its helpful to be
         * able to see what was created in the file system.
         */
        
        if(false && dataDir.exists() && dataDir.isDirectory()) {

            recursiveDelete( dataDir );
            
        }
        
    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself.
     * 
     * @param f
     *            A file or directory.
     */
    private void recursiveDelete(File f) {
        
        if(f.isDirectory()) {
            
            File[] children = f.listFiles();
            
            for(int i=0; i<children.length; i++) {
                
                recursiveDelete( children[i] );
                
            }
            
        }
        
        System.err.println("Removing: "+f);
        
        if (!f.delete())
            throw new RuntimeException("Could not remove: " + f);

    }
    
    /**
     * Verifies that two splits have the same data.
     * 
     * @param expected
     * @param actual
     */
    final public static void assertEquals(Split expected, Split actual) {
       
        assertEquals("partition",expected.pmd,actual.pmd);
        assertEquals("fromIndex",expected.fromIndex,actual.fromIndex);
        assertEquals("toIndex",expected.toIndex,actual.toIndex);
        assertEquals("ntuples",expected.ntuples,actual.ntuples);
        
    }
    
    /**
     * Verify that a named index is registered on a specific {@link DataService}
     * with the specified indexUUID.
     * 
     * @param dataService
     *            The data service.
     * @param name
     *            The index name.
     * @param indexUUID
     *            The unique identifier assigned to all instances of that index.
     */
    final protected void assertIndexRegistered(IDataService dataService, String name,
            UUID indexUUID) throws Exception {

        IndexMetadata metadata = dataService.getIndexMetadata(name,ITx.UNISOLATED);
        
        assertNotNull("metadata",metadata);
        
        assertEquals("indexUUID", indexUUID, metadata.getIndexUUID());
        
    }
    
    /**
     * Compares two byte[][]s for equality.
     * 
     * @param expected
     * @param actual
     */
    final public void assertEquals(byte[][] expected, byte[][] actual ) {
        
        assertEquals(null, expected, actual);
        
    }
    
    /**
     * Compares two byte[][]s for equality.
     * 
     * @param expected
     * @param actual
     */
    final public void assertEquals(String msg,byte[][] expected, byte[][] actual ) {
        
        if(msg==null) msg=""; else msg = msg+":";
        
        assertEquals(msg+"length", expected.length, actual.length);
        
        for( int i=0; i<expected.length; i++ ) {

            if (expected[i] == null) {

                if (actual[i] != null) {
                    
                    fail("expected " + i + "th entry to be null.");
                    
                }
                
            } else {

                if (BytesUtil.compareBytes(expected[i], actual[i]) != 0) {

                    fail("expected=" + BytesUtil.toString(expected[i])
                            + ", actual=" + BytesUtil.toString(actual[i]));

                }

            }
            
        }
        
    }

    /**
     * Waits until the overflow counter has been incremented, indicating that
     * overflow processing has occurred and that post-processing for the
     * overflow event is complete.
     * <p>
     * Note: Normally you use bring the data service to the brink of the desired
     * overflow event, note the current overflow counter using
     * {@link IDataService#getOverflowCounter()}, use
     * {@link IDataService#forceOverflow()} to set the forceOverflow flag, do
     * one more write to trigger group commit and overflow processing, and then
     * invoke this method to await the end of overflow post-processing.
     * 
     * @param dataService
     *            The data service.
     * 
     * @param priorOverflowCounter
     * 
     * @return The new value of the overflow counter.
     * 
     * @throws IOException
     */
    protected long awaitOverflow(IDataService dataService,long priorOverflowCounter) throws IOException {
        
        log.info("\n**** Awaiting overflow: priorOverflowCounter="+priorOverflowCounter+", service=" + dataService);

        final long begin = System.currentTimeMillis();

        long newOverflowCounter;
        
        while ((newOverflowCounter = dataService.getOverflowCounter()) == priorOverflowCounter) {

            try {
                Thread.sleep(250/* ms */);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.err.println("Still awaiting overflow: priorOverflowCounter="
                    + priorOverflowCounter + ", dataService=" + dataService);

            final long elapsed = System.currentTimeMillis() - begin;
            
            if(elapsed > 2000) fail("No overflow after "+elapsed+"ms?");
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        System.err.println("Overflow complete: elapsed=" + elapsed
                + " ms : priorOverflowCounter=" + priorOverflowCounter
                + ", newOverflowCounter=" + newOverflowCounter);

        assertTrue(newOverflowCounter > priorOverflowCounter);

        return newOverflowCounter;
        
    }
    
    /**
     * Return the #of index partitions in a scale-out index.
     * <p>
     * Note: This uses an key range scan to count only the non-deleted index
     * partition entries.
     * 
     * @param name
     *            The name of the scale-out index.
     * 
     * @return The #of index partitions.
     * 
     * @todo note that the {@link MetadataIndex} does not use delete markers so
     *       a range count would be exact.
     */
    protected int getPartitionCount(String name) {
        
        final ITupleIterator itr = new RawDataServiceRangeIterator(
                metadataService,//
                MetadataService.getMetadataIndexName(name), //
                ITx.READ_COMMITTED,//
                true, // readConsistent
                null, // fromKey
                null, // toKey
                0,    // capacity,
                IRangeQuery.DEFAULT,// flags
                null // filter
                );

        int n = 0;
        
        while(itr.hasNext()) {
            
            n++;
         
            log.info(SerializerUtil.deserialize(itr.next().getValue()));
            
        }
        
        return n;
        
    }
    
}
