/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jul 25, 2007
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import com.bigdata.btree.BTree;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITuple;
import com.bigdata.rawstore.Bytes;

/**
 * A utility class that opens the journal in a read-only mode and dumps the root
 * blocks and metadata about the indices on a journal file.
 * 
 * @todo add an option to collect histograms over index records so that "fat" in
 *       the indices may be targetted. We can always report histograms for the
 *       raw key and value data. However, with either an extensible serializer
 *       or with some application aware logic we are also able to report type
 *       specific histograms.
 * 
 * @todo add an option to dump only as of a specified commitTime?
 * 
 * @todo add an option to copy off data from one or more indices as of a
 *       specified commit time?
 * 
 * @todo add an option to restrict the names of the indices to be dumped (-name=<regex>).
 * 
 * @todo allow dump even on a journal that is open (e.g., only request a read
 *       lock or do not request a lock). An error is reported when you actually
 *       begin to read from the file once it is opened in a read-only mode if
 *       there is another process with an exclusive lock. In fact, since the
 *       root blocks must be consistent when they are read, a reader would have
 *       to have a lock at the moment that it read the root blocks...
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DumpJournal {

    public DumpJournal() {
        
    }
    
    /**
     * Dump one or more journal files.
     * 
     * @param args
     *            The name(s) of the journal file to open.
     *            <dl>
     *            <dt>-history </dt>
     *            <dd>Dump metadata for indices in all commit records (default
     *            only dumps the metadata for the indices as of the most current
     *            committed state).</dd>
     *            <dt>-indices</dt>
     *            <dd>Dump the records in the indices.</dd>
     *            </dl>
     */
    public static void main(String[] args) {
        
        if(args.length==0) {
            
            System.err.println("usage: (-history) <filename>+");
            
            System.exit(1);
            
        }

        int i = 0;
        
        boolean dumpHistory = false;
        
        boolean dumpIndices = false;
        
        for(; i<args.length; i++) {
            
            String arg = args[i];
            
            if( ! arg.startsWith("-")) {
                
                // End of options.
                break;
                
            }
            
            if(arg.equals("-history")) {
                
                dumpHistory = true;
                
            }
            
            if(arg.equals("-indices")) {
                
                dumpIndices = true;
                
            }
            
        }
        
        for(; i<args.length; i++) {
            
            final File file = new File(args[i]);

            try {

                dumpJournal(file,dumpHistory,dumpIndices);
                
            } catch( RuntimeException ex) {
                
                ex.printStackTrace();

                System.err.println("Error: "+ex+" on file: "+file);
                
            }
            
            System.err.println("==================================");

        }
        
    }
    
    public static void dumpJournal(File file,boolean dumpHistory,boolean dumpIndices) {
        
        /*
         * Stat the file and report on its size, etc.
         */
        {
            
          System.err.println("File: "+file);

          if(!file.exists()) {

              System.err.println("No such file");
              
              System.exit(1);
              
          }
          
          if(!file.isFile()) {
              
              System.err.println("Not a regular file");
              
              System.exit(1);
              
          }
          
          System.err.println("Length: "+file.length());

          System.err.println("Last Modified: "+new Date(file.lastModified()));
          
        }
        
        final Properties properties = new Properties();

        {
        
            properties.setProperty(Options.FILE, file.toString());
        
            properties.setProperty(Options.READ_ONLY, "" + true);
            
            properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
        }
        
        System.err.println("Opening (read-only): "+file);
        
        final Journal journal = new Journal(properties);

        try {
            
            FileMetadata fmd = journal.fileMetadata;

            // dump the MAGIC and VERSION.
            System.err.println("magic="+Integer.toHexString(fmd.magic));
            System.err.println("version="+Integer.toHexString(fmd.version));
            
            // dump the root blocks.
            System.err.println(fmd.rootBlock0.toString());
            System.err.println(fmd.rootBlock1.toString());

            // report on which root block is the current root block.
            System.err.println("The current root block is #"
                    + (journal.getRootBlockView().isRootBlock0() ? 0 : 1));
            
            /* 
             * Report on:
             * 
             * - the length of the journal.
             * - the #of bytes available for user data in the journal.
             * - the offset at which the next record would be written.
             * - the #of bytes remaining in the user extent.
             */

            final long bytesAvailable = (fmd.userExtent - fmd.nextOffset);
            
            System.err.println("extent="+fmd.extent+"("+fmd.extent/Bytes.megabyte+"M)"+
                    ", userExtent="+fmd.userExtent+"("+fmd.userExtent/Bytes.megabyte+"M)"+
                    ", bytesAvailable="+bytesAvailable+"("+bytesAvailable/Bytes.megabyte+"M)"+
                    ", nextOffset="+fmd.nextOffset);

            if (dumpHistory) {

                System.err.println("Historical commit points follow in temporal sequence (first to last):");
                
                CommitRecordIndex commitRecordIndex = journal._commitRecordIndex;
                
                ITupleIterator itr = commitRecordIndex.entryIterator();
                
                while(itr.hasNext()) {
                    
                    System.err.println("----");

                    final CommitRecordIndex.Entry entry = (CommitRecordIndex.Entry)itr.next();
                    
                    System.err.print("Commit Record: " + entry.commitTime
                            + ", addr=" + journal.toString(entry.addr)+", ");
                    
                    final ICommitRecord commitRecord = journal
                            .getCommitRecord(entry.commitTime);
                    
                    System.err.println(commitRecord.toString());

                    dumpNamedIndicesMetadata(journal,commitRecord,dumpIndices);
                    
                }
                
            } else {

                /*
                 * Dump the current commit record.
                 */
                
                final ICommitRecord commitRecord = journal.getCommitRecord();
                
                System.err.println(commitRecord.toString());

                dumpNamedIndicesMetadata(journal,commitRecord,dumpIndices);
                
            }

        } finally {

            journal.close();

        }

    }
    
    /**
     * Dump metadata about each named index as of the specified commit record.
     * 
     * @param journal
     * @param commitRecord
     */
    private static void dumpNamedIndicesMetadata(AbstractJournal journal,
            ICommitRecord commitRecord,boolean dumpIndices) {

        Name2Addr name2Addr = journal.name2Addr;

        ITupleIterator itr = name2Addr.entryIterator();

        while (itr.hasNext()) {

            Name2Addr.Entry entry = (Name2Addr.Entry) itr.next();

            System.err.print("name=" + entry.name + ", addr="
                    + journal.toString(entry.addr) + " : ");

            BTree ndx = (BTree) journal.getIndex(entry.name, commitRecord);

            if(ndx == null) {
                
                System.err.println("created.");
                
            } else {
            
                System.err.println(ndx.getIndexMetadata().toString());
                
                if(dumpIndices) dumpIndex(ndx);
                
            }
            
        }
        
    }

    private static void dumpIndex(BTree btree) {

        // @todo offer the version metadata also if the index supports isolation.
        ITupleIterator itr = btree.rangeIterator(null, null);
        
        int i = 0;
        
        while(itr.hasNext()) {
            
            ITuple tuple = itr.next();

            byte[] key = tuple.getKey();
            
            byte[] val = tuple.getValue();
            
            System.err.println("rec="+i+"\n key="+dumpKey(key)+"\n val="+dumpVal(val));
            
            i++;
            
        }
        
    }

    private static String dumpKey(Object key) {
        
        try {

            byte[] tmp = (byte[]) key;

            return Arrays.toString(tmp);
            
        } catch (ClassCastException ex) {
            
            return ""+key;
            
        }
        
    }
    
    private static String dumpVal(Object key) {
        
        try {

            byte[] tmp = (byte[]) key;

            return Arrays.toString(tmp);
            
        } catch (ClassCastException ex) {
            
            return ""+key;
            
        }
        
    }

}
