package benchmark.bigdata;

import java.io.File;
import java.util.Properties;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;
import benchmark.testdriver.TestDriver;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

public class BigdataQueryDriver {
    /**
     * @param args
     *          USAGE: -journal <journal file> -data <data file>
     */
    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.err.println("USAGE: <bsbm args...> -journal <journal file>");
            }
            String journal = args[args.length-1];
            File file = new File(journal);
            if (!file.exists()) {
                throw new RuntimeException("could not find the journal");
            }
            
            String[] bsbmArgs = new String[args.length-2];
            System.arraycopy(args, 0, bsbmArgs, 0, bsbmArgs.length);
            
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
            
            // run TestDriver with bsbmArgs, repo
            TestDriver.main(bsbmArgs, repo);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
}
