package benchmark.bigdata;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;

/**
 * Very simple loader that takes a journal file and a data file as input and
 * loads the data into the journal.  If the journal file exists, it will be
 * destroyed before the load so that the load takes place on a clean journal.
 * 
 * @todo Should be replaced with a bulk loader for scale out.
 * 
 * @author mike
 */
public class BigdataLoader {

    /**
     * @param args
     *            USAGE: <code>propertyFile dataFile</code><br>
     *            where <i>propertyFile</i> contains the default properties which
     *            will be used to initialize the database and knowledge base
     *            instance<br>
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
        Journal jnl = null;
        try {
            if (args.length != 2) {
                System.err.println("USAGE: propertyFile dataFile");
                System.exit(1);
            }
            final File propertyFile = new File(args[0]);
            final String dataFile = args[1];
            
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
            
//            properties.setProperty(BigdataSail.Options.BUFFER_MODE,BufferMode.Disk.toString());// Disk is default.
//            properties.setProperty(BigdataSail.Options.QUADS, "false");
//            properties.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
//            properties.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
//            properties.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
//            properties.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
//            properties.setProperty(BigdataSail.Options.TERM_CACHE_CAPACITY, "50000"); // 50000 default.
//            properties.setProperty(BigdataSail.Options.BUFFER_CAPACITY, "100000"); // 10000 default.
////            properties.setProperty(BigdataSail.Options.NESTED_SUBQUERY, "false"); // false is the pipeline join, which is much better.
////            properties.setProperty(BigdataSail.Options.MAX_PARALLEL_SUBQUERIES, "5"); // 5 is default, only applies to nextedSubquery joins.
//            properties.setProperty(BigdataSail.Options.CHUNK_CAPACITY,"100"); // 100 default.
//            properties.setProperty(BigdataSail.Options.CHUNK_OF_CHUNKS_CAPACITY,"1000"); // 1000 default.
//            properties.setProperty(com.bigdata.btree.IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "8000");
//            properties.setProperty(com.bigdata.journal.Options.INITIAL_EXTENT, ""+(1048576*200)); // 200M initial extent.
////            properties.setProperty(BigdataSail.Options.FILE, journalFile.getAbsolutePath());

            final File journalFile = new File(properties.getProperty(BigdataSail.Options.FILE));
            if (journalFile.exists()) {
                if(!journalFile.delete()) {
                    throw new RuntimeException("could not delete old journal file");
                }
            }
            System.out.println("Journal: "+journalFile);

            // Create the journal (we know it does not exist per above).
            jnl = new Journal(properties);
            
            // Create the KB instance (we know that this is a new journal).
            final String namespace = "kb";  // default namespace.
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
     * @todo this is not an efficient API for loading the data.
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
