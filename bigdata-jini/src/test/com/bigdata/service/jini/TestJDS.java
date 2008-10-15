package com.bigdata.service.jini;

import java.io.File;
import java.util.Properties;

import com.bigdata.journal.AbstractIndexManagerTestCase;
import com.bigdata.journal.ProxyTestCase;
import com.bigdata.service.EmbeddedFederation;

/**
 * Delegate for {@link ProxyTestCase}s for services running against an
 * {@link EmbeddedFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestJDS extends
        AbstractIndexManagerTestCase<JiniFederation> {

    /**
     * 
     */
    public TestJDS() {
        super();
    }

    /**
     * @param name
     */
    public TestJDS(String name) {
        super(name);
    }

    private File dataDir;
    
    /**
     * Data files are placed into a directory named by the test. If the
     * directory exists, then it is removed before the federation is set up.
     */
    public void setUp(ProxyTestCase testCase) throws Exception {
      
        final String name = testCase.getName();
        
        assert name != null;

        dataDir = new File(name);
      
        if(dataDir.exists() && dataDir.isDirectory()) {
        
            recursiveDelete( dataDir );
            
        }

        services = new JiniServicesHelper(path);
        
        try {
        
            services.start();
            
        } catch (Throwable t) {
            
            log.error("Could not start: " + t);
            
            services.destroy();
            
            throw new RuntimeException("Could not start", t);
                        
        }
        
    }
    private String path = "src/resources/config/standalone/";
    private JiniServicesHelper services;
    
    /**
     * Optional cleanup after the test runs, but sometimes its helpful to be
     * able to see what was created in the file system.
     */
    public void tearDown(ProxyTestCase testCase) throws Exception {

//        if(true && dataDir.exists() && dataDir.isDirectory()) {
//
//            recursiveDelete( dataDir );
//            
//        }

//        services.destroy();
        
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
            
            final File[] children = f.listFiles();

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(children[i]);
                
            }
            
        }
        
        System.err.println("Removing: "+f);
        
        if (!f.delete())
            throw new RuntimeException("Could not remove: " + f);

    }
    
    @Override
    protected JiniFederation getStore(Properties properties) {

        return services.client.connect();
    }

    @Override
    protected JiniFederation reopenStore(JiniFederation fed) {

        log.warn("Reopen not supported.");
        
        return fed;
        
//        services.shutdown();
//        
//        services = new JiniServicesHelper(path);
//        
//        services.start();
//        
//        return services.client.connect();
                
    }
    
}
