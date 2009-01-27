package com.bigdata.rdf.load;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Serializable;

import org.openrdf.rio.RDFFormat;

/**
 * Filter recognizes anything that is a registered as an {@link RDFFormat}.
 * If it does not recognize your format, then you can create a subclass
 * which ensures the static initialization of your format with
 * {@link RDFFormat}. That needs to be done in static code so that it will
 * be performed on the machine where this filter is being used.
 */
public class RDFFilenameFilter implements FilenameFilter, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -628798437502907063L;

    public boolean accept(File dir, String name) {

        if (new File(dir, name).isDirectory()) {

            // visit subdirectories.
            return true;

        }

        // if recognizable as RDF.
        return RDFFormat.forFileName(name) != null;

    }

}