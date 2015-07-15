package com.bigdata.rdf.lexicon;

import java.io.File;

import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.util.DumpLexicon;
import com.bigdata.service.jini.JiniClient;

/**
 * Utility class to dump the TERMS index of a triple store.
 * 
 * BLZG-1370  Moved into bigdata-jini package for the remove functionality.
 * 
 * Includes the functionality of the base
 * 
 * @author thompsonbry
 */
public class DumpRemoteLexicon  extends DumpLexicon {

    /**
     * Open the {@link IIndexManager} identified by the property file.
     * 
     * @param propertyFile
     *            The property file (for a standalone bigdata instance) or the
     *            jini configuration file (for a bigdata federation). The file
     *            must end with either ".properties" or ".config".
     *            
     * @return The {@link IIndexManager}.
     */
    protected static IIndexManager openIndexManager(final String propertyFile) {

        final File file = new File(propertyFile);

        if (!file.exists()) {

            throw new RuntimeException("Could not find file: " + file);

        }

        final IIndexManager indexManager;

        try {
        if (propertyFile.endsWith(".config")) {
            // scale-out with Jini
                @SuppressWarnings("unchecked")
                final JiniClient<?> jiniClient = new JiniClient(
                        new String[] { propertyFile });

                indexManager = jiniClient.connect();
        } else {
        	indexManager = DumpLexicon.openIndexManager(propertyFile);
        }
        

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

        return indexManager;
        
    }
}
