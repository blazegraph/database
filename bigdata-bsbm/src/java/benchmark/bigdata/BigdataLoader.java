package benchmark.bigdata;

import java.io.File;
import java.util.Properties;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * Very simple loader that takes a journal file and a data file as input and
 * loads the data into the journal.  If the journal file exists, it will be
 * destroyed before the load so that the load takes place on a clean journal.
 * <p>
 * Should be replaced with a bulk loader for scale out.
 * 
 * @author mike
 */
public class BigdataLoader {
    
    /**
     * @param args
     *          USAGE: -journal <journal file> -data <data file>
     */
    public static void main(String[] args) {
        try {
            if (args.length < 4) {
                System.err.println("USAGE: -journal <journal file> -data <data file>");
            }
            String journal = args[1];
            String data = args[3];
            File file = new File(journal);
            if (file.exists()) {
                file.delete();
            }
            if (file.exists()) {
                throw new RuntimeException("could not delete old journal file");
            }
            
            Properties properties = new Properties();
            properties.setProperty(
                    BigdataSail.Options.QUADS, "false");
            properties.setProperty(
                    BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
            properties.setProperty(
                    BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
            properties.setProperty(
                    BigdataSail.Options.TRUTH_MAINTENANCE, "false");
            properties.setProperty(
                    BigdataSail.Options.FILE, file.getAbsolutePath());

            BigdataSail sail = new BigdataSail(properties);
            BigdataSailRepository repo = new BigdataSailRepository(sail);
            repo.initialize();
            
            loadData(repo, data);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    /**
     * Load a data file into a SAIL via the Sesame Repository API.
     * 
     * @param sail
     *          the SAIL
     * @param data
     *          path to the data (assumes ntriples)
     */
    private static final void loadData(BigdataSailRepository repo, String data) 
            throws Exception {
        
        RepositoryConnection cxn = null;
        try {
            // setup
            cxn = repo.getConnection();
            cxn.setAutoCommit(false);
            String baseURI = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01";
            long start = System.currentTimeMillis();

            // perform the load
            cxn.add(new File(data), baseURI, RDFFormat.NTRIPLES);
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
