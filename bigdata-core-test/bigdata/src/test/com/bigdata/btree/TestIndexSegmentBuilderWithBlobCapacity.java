package com.bigdata.btree;

import java.io.File;
import java.io.IOException;

import com.bigdata.btree.keys.TestKeyBuilder;

public class TestIndexSegmentBuilderWithBlobCapacity extends
        AbstractIndexSegmentTestCase {

    File outFile;

    File tmpDir;

    final boolean bufferNodes = true;

    public void setUp() throws Exception {

        super.setUp();
        
        outFile = new File(getName() + ".seg");

        if (outFile.exists() && !outFile.delete()) {

            throw new RuntimeException("Could not delete file: " + outFile);

        }
        
        tmpDir = outFile.getAbsoluteFile().getParentFile();

    }

    public void tearDown() throws Exception {

        if (outFile != null && outFile.exists() && !outFile.delete()) {

            log.warn("Could not delete file: " + outFile);

        }

        super.tearDown();
        
    }
    
    public void test_emptyBlobFile() {
    	doTestBlobFile(0);
    }
    
    public void test_smallBlobFile() {
    	doTestBlobFile(5);
    }
    
    public void test_largeBlobFile() {
    	doTestBlobFile(1000);
    }
    
    public void doTestBlobFile(final int entries) {
    	try {
			outFile.createNewFile();
			outFile.deleteOnExit();
		} catch (IOException e) {
			fail("Unable to create the file", e);
		}
		
		BTree btree = getBTree(3);
		for (int i = 0; i < entries; i++) {
			btree.insert(TestKeyBuilder.asSortKey(i), new SimpleEntry(i));
		}
        try {
			doBuildAndDiscardCache(btree, 3);
		} catch (Exception e) {
			fail("Unable to build cache", e);
		}
		
        final IndexSegmentStore segStore = new IndexSegmentStore(outFile);
        try {
        	final IndexSegment seg = segStore.loadIndexSegment();
        } finally {
        	segStore.close();
        }
    }
    
    @Override
    protected boolean useRawRecords() {
        return true;
    }

    protected IndexSegmentCheckpoint doBuildAndDiscardCache(final BTree btree,
            final int m) throws IOException, Exception {

        final long commitTime = System.currentTimeMillis();

        final IndexSegmentCheckpoint checkpoint = IndexSegmentBuilder
                .newInstance(outFile, tmpDir, btree.getEntryCount(),
                        btree.rangeIterator(), m, btree.getIndexMetadata(),
                        commitTime, true/* compactingMerge */, bufferNodes)
                .call();

//      @see BLZG-1501 (remove LRUNexus)
//        if (LRUNexus.INSTANCE != null) {
//
//            /*
//             * Clear the records for the index segment from the cache so we will
//             * read directly from the file. This is necessary to ensure that the
//             * data on the file is good rather than just the data in the cache.
//             */
//            
//            LRUNexus.INSTANCE.deleteCache(checkpoint.segmentUUID);
//
//        }
        
        return checkpoint;

    }
    
}
