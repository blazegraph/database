/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Nov 3, 2008
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;


/**
 * Utility class to compact a {@link Journal}. The {@link Journal} can not be
 * open in another application.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CompactJournalUtility {

    static public void usage() {
    
        System.err.println("usage: inFile outFile");
        
        System.exit( 1 );
        
    }
    
    /**
     * Compacts the source journal, writing a new journal containing only the
     * most current committed view onto the specified output file.
     * 
     * @param args
     *            <i>inFile</i> <i>outFile</i>
     */
    static public void main(final String[] args) {
        
        if (args.length != 2) {
            
            usage();
            
        }

        final File inFile = new File(args[0]);
        
        final File outFile = new File(args[1]);
        
        if(!inFile.exists()) {
            
            System.err.println("Source file does not exist: "+inFile);
            
            System.exit(1);
            
        }
        
        if (outFile.exists() && outFile.length() > 0L) {

            System.err.println("Output file exists and is not empty: " + outFile);

            System.exit(1);
            
        }
        
        System.out.println("inFile=" + inFile + ", outFile=" + outFile);
            
        final Journal sourceJournal;
        {
            Properties p = new Properties();

            p.setProperty(Options.FILE, inFile.getAbsolutePath());

            sourceJournal = new Journal(p);
        
            System.out.println("source: nbytes="
                    + sourceJournal.size()
                    + ", nindices="
                    + sourceJournal.getName2Addr(
                            sourceJournal.getLastCommitTime()).rangeCount());
        
        }

        try {

            final Future<Journal> f = sourceJournal.compact(outFile);

            System.out.println("Running: " + new Date());

            final Journal newJournal = f.get();
            
            System.out.println("Success: " + new Date());

            final long bytesBefore = sourceJournal.size();
            final long bytesAfter = newJournal.size();
            final int percentChange = 100 - (int) (bytesAfter * 100d / bytesBefore);
            
            System.out.println("bytes used: before=" + bytesBefore + ", after="
                    + bytesAfter + ", reducedBy=" + percentChange);
            
            /*
             * Immediate shutdown since nothing should be running on the newly
             * created journal.
             */
            newJournal.shutdownNow();

            // success.
            System.exit(0);
                
        } catch (Throwable t) {

            sourceJournal.shutdownNow();

        }

        sourceJournal.shutdown();
        
        System.exit(0);

    }

}
