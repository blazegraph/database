import info.aduna.iteration.CloseableIteration;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.sail.BigdataSail;

/**
 * This class shows how to initialize the SAIL for use with Sesame 2.x. The SAIL
 * is backed by a "journal" file, and there is a property which you can set to
 * specify where that file lives. You can use SAILs with or without inference,
 * but this demonstrates the use of the RDFSchemaRepository SAIL - with
 * inference.
 * <p>
 * This class also turns down the logging level -- the default logging level is
 * DEBUG, and bigdata generates a huge amount of logging at that level so we
 * always, always, always turn this down when we deploy anything. Normally you
 * would configure the log level using <code>-Dlog4j.configuration=URL</code>
 * and given the URL of the log4j configuration resource, e.g.,
 * <code>file:/log4j.properties</code>. However we have done it in the code
 * here so that the class will execute at a reasonable log level regardless of
 * the configured environment.
 * <p>
 * We tend to use either a recent Sun or JRockit JVM with the -server option.
 * Assuming that you have 3G+ of RAM on your machine, you can increase the
 * memory limit to 1 or 2G for the JVM. However, bigdata gets by quite well on
 * less memory when handling smaller data sets (10s - 100s of millions).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Test {

    static URI mike = new URIImpl( "http://www.systap.com/rdf#Mike" );
    static URI knows = new URIImpl( "http://www.systap.com/rdf#knows" );
    static URI bryan = new URIImpl( "http://www.systap.com/rdf#Bryan" );
    
	public static final void main( String[] args ) {
		
		try {

            /*
             * Bigdata generates a log of conditional logging at INFO or
             * DEBUG so this turns down the logging level if it appears
             * to be at its default level.
             */
            
            Logger log = Logger.getRootLogger();

            if (log.getLevel().equals(Level.DEBUG)) {
                
                log.setLevel(Level.WARN);

                log.warn("Defaulting debugging level to WARN.");

            }
            
            /*
             * Setup SAIL supporting RDF Schema.
             */
            
            Properties properties = new Properties();
            
            properties.setProperty(BigdataSail.Options.FILE, "bigdata.jnl");
            
            BigdataSail sail = new BigdataSail(properties);

            sail.initialize();

            SailConnection conn = sail.getConnection();
            
			conn.addStatement( 
			    mike, 
                knows,
                bryan
                );
            
            conn.commit();

            /*
             * Dump the KB, including all entailments.
             */
            CloseableIteration<? extends Statement, SailException> it = conn
                    .getStatements(null, null, null, true/* includeInferred */);
            
            while ( it.hasNext() ) {
                
                Statement stmt = it.next();
                
                System.out.println( stmt );
                
            }
            
        } catch ( Exception ex ) {
            
            ex.printStackTrace();
            
        }
        
    }
    
}
