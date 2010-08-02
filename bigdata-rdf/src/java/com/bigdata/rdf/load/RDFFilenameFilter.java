package com.bigdata.rdf.load;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Serializable;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.rio.NQuadsParser;

/**
 * Filter recognizes anything that is a registered as an {@link RDFFormat} or
 * which would be recognized as such if you stripped off a trailing
 * <code>.zip</code> or <code>.gz</code> filename extension. If it does not
 * recognize your format, then you can create a subclass which ensures the
 * static initialization of your format with {@link RDFFormat}. That needs to
 * be done in static code so that it will be performed on the machine where this
 * filter is being used.
 */
public class RDFFilenameFilter implements FilenameFilter, Serializable {

    final protected transient static Logger log = Logger
            .getLogger(RDFFilenameFilter.class);

    /**
     * 
     */
    private static final long serialVersionUID = -628798437502907063L;

    /**
     * Force the load of the NxParser integration class and its registration
     * of the NQuadsParser#nquads RDFFormat.
     * 
     * @todo Should be done via META-INFO.
     */
    static {

        // Force the load of the NXParser integration.
        NQuadsParser.forceLoad();
        
    }
    
    public boolean accept(final File dir, final String name) {

        final File file = new File(dir, name);
        
        if (file.isHidden()) {

            return false;
            
        }
        
        if (file.isDirectory()) {

            // visit subdirectories.
            return true;

        }

        if (RDFFormat.forFileName(name) != null) {
            // recognizable as RDF.
            return true;
        }
        
        if (name.endsWith(".gz")) {
            final String s = name.substring(0, name.length() - 3);
            if (RDFFormat.forFileName(s) != null) {
                // recognizable as gzip'd RDF.
                return true;
            }
        }
        
        if (name.endsWith(".zip")) {
            final String s = name.substring(0, name.length() - 4);
            if (RDFFormat.forFileName(s) != null) {
                // recognizable as gzip'd RDF.
                return true;
            }
        }
        
        return false;

    }

}
