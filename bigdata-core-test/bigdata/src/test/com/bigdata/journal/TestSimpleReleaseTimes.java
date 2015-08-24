package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import com.bigdata.btree.BTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.service.AbstractClient;

/**
 * Example of Commit History usage.
 * 
 * A series of delayed commits stores different values against a named index 
 * and stores the commit time of each.
 * 
 * Then the store is closed and re-opened. Looking up the historical values stored
 * against the previous commit times.
 * 
 * New data is then committed, resulting in aged records being released and a
 * correct failure to retrieve historical state.
 * 
 * @author Martyn Cutcher
 *
 */
public class TestSimpleReleaseTimes {

	public static void main(String[] args) throws IOException, InterruptedException {
        
		Journal journal = new Journal(getProperties("2000"));

		final String name = "TestIndex";
		
		final IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

		final BTree ti = journal.registerIndex(name, md);

        journal.commit();
        
        final long start = journal.getLastCommitTime();
        
        // Add simple values and confirm read back
        
        final long bryan = insertName(journal, "Bryan");        
        final long mike = insertName(journal, "Mike");              
        final long martyn = insertName(journal, "Martyn");
        
        // Retrieve historical index
        assert checkState(journal, bryan, "Bryan");
        assert checkState(journal, mike, "Mike");
        assert checkState(journal, martyn, "Martyn");
        
        // Reopen the journal
        journal.shutdown();        
        journal = new Journal(getProperties("2000"));
        
        // wait for data to "age"
		Thread.sleep(3000);

        // ... confirm historical access - despite no current historical protection
        assert checkState(journal, bryan, "Bryan");
        assert checkState(journal, mike, "Mike");
        assert checkState(journal, martyn, "Martyn");
        
        // Now we will make a further commit that will
        //	release historical index state.
        System.out.println("Current commit time: " + insertName(journal, "SomeoneElse"));
        // FIXME Apparently a further commit is required 
        // System.out.println("Current commit time: " + insertName(journal, "Another"));
        
        // ... confirm no historical access

        assert !checkState(journal, bryan, "Bryan");
        assert !checkState(journal, mike, "Mike");
        assert !checkState(journal, martyn, "Martyn");

        System.out.println("DONE");
	}
	
	/**
	 * Add the value to the index, commit, assert value retrieval and
	 * return the commit time
	 */
	static long insertName(final Journal jrnl, final String name) {
        IIndex index = jrnl.getIndex("TestIndex");
		index.remove("Name");
		index.insert("Name", name);
		
		final long ret = jrnl.commit();
		
		if(!name.equals(index.lookup("Name"))) {
			throw new AssertionError(name + " != " + index.lookup("Name"));
		}
		
		return ret;
	}
	
	static boolean checkState(final Journal jrnl, final long state, final String name) {
		try {
	        IIndex istate = jrnl.getIndex("TestIndex", state);
	        
	        if (istate == null) {
	        	System.out.println("No index found for state: " + state);
	        	
	        	return false;
	        }
	        
			if(!name.equals(istate.lookup("Name"))) {
				throw new AssertionError(name + " != " + istate.lookup("Name"));
			}
			
        	System.out.println("Index confirmed for state: " + state);

        	return true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new AssertionError("Unable to load index");
		}
	}
	
	static String filename = null;
    static public Properties getProperties(String releaseAge) throws IOException {
        
        Properties properties = new Properties();
        
        // create temporary file for this application run
        if (filename == null)
        	filename = File.createTempFile("BIGDATA", "jnl").getAbsolutePath();

        properties.setProperty(Options.FILE, filename);
        properties.setProperty(Options.DELETE_ON_EXIT,"true");
            
        // Set RWStore
        properties.setProperty(Options.BUFFER_MODE, BufferMode.DiskRW.toString());
        
        // Set minimum commit history
        properties.setProperty("com.bigdata.service.AbstractTransactionService.minReleaseAge", releaseAge);
       
        return properties;
        
    }
}
