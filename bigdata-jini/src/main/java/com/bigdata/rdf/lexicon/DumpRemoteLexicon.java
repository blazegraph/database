package com.bigdata.rdf.lexicon;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Properties;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.util.DumpLexicon;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
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
