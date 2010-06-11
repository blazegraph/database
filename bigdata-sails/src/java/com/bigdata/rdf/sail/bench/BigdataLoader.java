package com.bigdata.rdf.sail.bench;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.load.RDFDataLoadMaster;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Very simple loader that takes a journal file and a data file as input and
 * loads the data into the journal. 
 * <p>
 * If the journal file exists, you should generally delete it 
 * before the load so that the load takes place on a clean journal.
 * <p>
 * Note: For scale-out, use the {@link RDFDataLoadMaster} instead.
 * 
 * @author mike
 * @author thompsonbry
 */
public class BigdataLoader {

    /**
     * @param args
     *            USAGE: <code>namespace propertyFile dataFile</code><br>
     *            where <i>namespace</i> is the namespace of the target KB
     *            instance><br>
     *            where <i>propertyFile</i> contains the default properties
     *            which will be used to initialize the database and knowledge
     *            base instance<br>
     *            where <i>dataFile</i> is the name of the file to be loaded.
     *            The file extensions <code>.gz</code> and <code>.zip</code> are
     *            supported.
     *            <p>
     *            Note: The name of the journal file may be specified or
     *            overridden using
     *            <code>-Dcom.bigdata.journal.AbstractJournal.File=...</code>.
     *            All other arguments should be specified in the property file.
     */
    public static void main(final String[] args) {
        if (args.length != 3) {
            System.err.println("USAGE: namespace propertyFile dataFile");
            System.exit(1);
        }
        Journal jnl = null;
        try {
            final String namespace = args[0];
            final File propertyFile = new File(args[1]);
            final String dataFile = args[2];
            
            final Properties properties = new Properties();
            {
                // Read the properties from the file.
                final InputStream is = new BufferedInputStream(
                        new FileInputStream(propertyFile));
                try {
                    properties.load(is);
                } finally {
                    is.close();
                }
                if (System.getProperty(BigdataSail.Options.FILE) != null) {
                    // Override/set from the environment.
                    properties.setProperty(BigdataSail.Options.FILE, System
                            .getProperty(BigdataSail.Options.FILE));
                }
            }
            
            final File journalFile;
            {
                final String tmp = properties
                        .getProperty(BigdataSail.Options.FILE);
                if (tmp == null) {
                    throw new RuntimeException(
                            "Required property not specified: "
                                    + BigdataSail.Options.FILE);
                }
                journalFile = new File(tmp);
                if (journalFile.exists()) {
                    if (!journalFile.delete()) {
                        throw new RuntimeException(
                                "could not delete old journal file");
                    }
                }
                System.out.println("Journal: " + journalFile);
            }

            // Create the journal (we know it does not exist per above).
            jnl = new Journal(properties);
            
            // Create the KB instance (we know that this is a new journal).
            final AbstractTripleStore kb = new LocalTripleStore(jnl, namespace, ITx.UNISOLATED, properties);
            kb.create();
            
            // Wrap the kb as a Sail and then wrap the Sail as a SailRepository.
            final BigdataSail sail = new BigdataSail(kb);
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();

            // Load the data into the repository.
            loadData(repo, dataFile);

            // Truncate the file on the disk to the actual used extent.
            jnl.truncate();
            
        } catch (Exception ex) {
            
            ex.printStackTrace();
            
        } finally {

            if (jnl != null) {

                jnl.close();
                
            }
            
        }
        
    }

    /**
     * Load a data file into a SAIL via the Sesame Repository API.
     * 
     * @param sail
     *            the SAIL
     * @param data
     *            path to the data (assumes ntriples)
     * 
     * @todo This is not an efficient API for loading the data.  use the 
     * 		 {@link DataLoader} instead.
     */
    private static final void loadData(BigdataSailRepository repo, String data) 
            throws Exception {
        
        RepositoryConnection cxn = null;
        try {
            RDFFormat format = null;
            if (data.toLowerCase().endsWith(".ttl")) {
                format = RDFFormat.TURTLE;
            } else if (data.toLowerCase().endsWith(".nt")) {
                format = RDFFormat.NTRIPLES;
            }  
            
            // setup
            cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            String baseURI = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01";
            long start = System.currentTimeMillis();

            // perform the load
            cxn.add(new File(data), baseURI, format);
            cxn.commit();
        
            // report throughput
            long duration = System.currentTimeMillis() - start;
            long size = cxn.size();
            System.err.println("loaded " + size + " triples in " + duration + " millis.");
            long tps = (long) (((double) size) / ((double) duration) * 1000d);
            System.err.println("tps: " + tps);
            
        } finally {
            cxn.close();
        }
        
    }
    
}
