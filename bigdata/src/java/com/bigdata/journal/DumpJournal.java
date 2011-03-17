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
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.BTree;
import com.bigdata.btree.DumpIndex;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.DumpIndex.PageStats;
import com.bigdata.rawstore.Bytes;
import com.bigdata.util.InnerCause;

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

    protected static final Logger log = Logger.getLogger(DumpJournal.class);
    
//    protected static final boolean INFO = log.isInfoEnabled();
    
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
     *            <dd>Dump the indices (does not show the tuples by default).</dd>
     *            <dt>-pages</dt>
     *            <dd>Dump the pages of the indices and reports some information on the page size.</dd>
     *            <dt>-tuples</dt>
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

        boolean dumpPages = false;
        
        boolean showTuples = false;
        
        for(; i<args.length; i++) {
            
            String arg = args[i];
            
            if( ! arg.startsWith("-")) {
                
                // End of options.
                break;
                
            }
            
            if(arg.equals("-history")) {
                
                dumpHistory = true;
                
            }
            
            else if(arg.equals("-indices")) {
                
                dumpIndices = true;
                
            }

            else if(arg.equals("-pages")) {
                
                dumpPages = true;
                
            }

            else if(arg.equals("-tuples")) {
                
                showTuples = true;
                
            }
            
			else
				throw new RuntimeException("Unknown argument: " + arg);
            
        }
        
        for(; i<args.length; i++) {
            
            final File file = new File(args[i]);

            try {

                dumpJournal(file,dumpHistory,dumpPages,dumpIndices,showTuples);
                
            } catch( RuntimeException ex) {
                
                ex.printStackTrace();

                System.err.println("Error: "+ex+" on file: "+file);
                
            }
            
            System.out.println("==================================");

        }
        
    }
    
    public static void dumpJournal(File file,boolean dumpHistory,boolean dumpPages,boolean dumpIndices,boolean showTuples) {
        
        /*
         * Stat the file and report on its size, etc.
         */
        {
            
          System.out.println("File: "+file);

          if(!file.exists()) {

              System.err.println("No such file");
              
              System.exit(1);
              
          }
          
          if(!file.isFile()) {
              
              System.err.println("Not a regular file");
              
              System.exit(1);
              
          }
          
          System.out.println("Length: "+file.length());

          System.out.println("Last Modified: "+new Date(file.lastModified()));
          
        }
        
        final Properties properties = new Properties();

        {
        
            properties.setProperty(Options.FILE, file.toString());
        
            properties.setProperty(Options.READ_ONLY, "" + true);
            
            properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
        }
        
        System.out.println("Opening (read-only): "+file);
        
        final Journal journal = new Journal(properties);

        try {
            
            final FileMetadata fmd = journal.getFileMetadata();

            // dump the MAGIC and VERSION.
            System.out.println("magic="+Integer.toHexString(fmd.magic));
            System.out.println("version="+Integer.toHexString(fmd.version));
            
            // dump the root blocks.
            System.out.println(fmd.rootBlock0.toString());
            System.out.println(fmd.rootBlock1.toString());

            // report on which root block is the current root block.
            System.out.println("The current root block is #"
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
            
            System.out.println("extent="+fmd.extent+"("+fmd.extent/Bytes.megabyte+"M)"+
                    ", userExtent="+fmd.userExtent+"("+fmd.userExtent/Bytes.megabyte+"M)"+
                    ", bytesAvailable="+bytesAvailable+"("+bytesAvailable/Bytes.megabyte+"M)"+
                    ", nextOffset="+fmd.nextOffset);

            if (dumpHistory) {

                System.out.println("Historical commit points follow in temporal sequence (first to last):");
                
                final CommitRecordIndex commitRecordIndex = journal.getCommitRecordIndex();
//                CommitRecordIndex commitRecordIndex = journal._commitRecordIndex;
                
                final ITupleIterator<CommitRecordIndex.Entry> itr = commitRecordIndex.rangeIterator();
                
                while(itr.hasNext()) {
                    
                    System.out.println("----");

                    final CommitRecordIndex.Entry entry = itr.next().getObject();
                    
                    System.out.print("Commit Record: " + entry.commitTime
                            + ", addr=" + journal.toString(entry.addr)+", ");
                    
                    final ICommitRecord commitRecord = journal
                            .getCommitRecord(entry.commitTime);
                    
                    System.out.println(commitRecord.toString());

                    dumpNamedIndicesMetadata(journal,commitRecord,dumpPages,dumpIndices,showTuples);
                    
                }
                
            } else {

                /*
                 * Dump the current commit record.
                 */
                
                final ICommitRecord commitRecord = journal.getCommitRecord();
                
                System.out.println(commitRecord.toString());

                dumpNamedIndicesMetadata(journal,commitRecord,dumpPages,dumpIndices,showTuples);
                
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
			ICommitRecord commitRecord, boolean dumpPages, boolean dumpIndices, boolean showTuples) {

        // view as of that commit record.
        final IIndex name2Addr = journal.getName2Addr(commitRecord.getTimestamp());

        final ITupleIterator itr = name2Addr.rangeIterator(null,null);
        
		final Map<String, PageStats> pageStats = dumpPages ? new TreeMap<String, PageStats>()
				: null;
		
        while (itr.hasNext()) {

            // a registered index.
            final Name2Addr.Entry entry = Name2Addr.EntrySerializer.INSTANCE
                    .deserialize(itr.next().getValueStream());

            System.out.println("name=" + entry.name + ", addr="
                    + journal.toString(entry.checkpointAddr));

            // load B+Tree from its checkpoint record.
            final BTree ndx;
            try {
                
                ndx = (BTree) journal.getIndex(entry.checkpointAddr);
                
            } catch (Throwable t) {

                if (InnerCause.isInnerCause(t, ClassNotFoundException.class)) {

                    /*
                     * This is typically a tuple serializer that has a
                     * dependency on an application class that is not present in
                     * the CLASSPATH. Add the necessary dependency(s) and you
                     * should no longer see this message.
                     */
                    
                    log.warn("Could not load index: "
                            + InnerCause.getInnerCause(t,
                                    ClassNotFoundException.class));
                    
                    continue;
                    
                } else
                    throw new RuntimeException(t);
                
            }

            // show checkpoint record.
            System.out.println("\t" + ndx.getCheckpoint());

            // show metadata record.
            System.out.println("\t" + ndx.getIndexMetadata());

			if (pageStats != null) {
				
				final PageStats stats = DumpIndex
						.dumpPages(ndx, false/* dumpNodeState */);

				System.out.println("\t" + stats);

				pageStats.put(entry.name, stats);

			}

            if (dumpIndices)
                DumpIndex.dumpIndex(ndx, showTuples);

        }

		if (pageStats != null) {

			/*
			 * TODO If we kept the BTree counters for the #of bytes written per
			 * node and per leaf up to date when nodes and leaves were recycled
			 * then we could generate this table very quickly. As it stands, we
			 * have to actually scan the pages in the index.
			 */
			System.out.print("name");
			System.out.print('\t');
			System.out.print("m");
			System.out.print('\t');
			System.out.print("height");
			System.out.print('\t');
			System.out.print("nnodes");
			System.out.print('\t');
			System.out.print("nleaves");
			System.out.print('\t');
			System.out.print("nodeBytes");
			System.out.print('\t');
			System.out.print("leafBytes");
			System.out.print('\t');
			System.out.print("totalBytes");
			System.out.print('\t');
			System.out.print("avgNodeBytes");
			System.out.print('\t');
			System.out.print("avgLeafBytes");
			System.out.print('\t');
			System.out.print("minNodeBytes");
			System.out.print('\t');
			System.out.print("maxNodeBytes");
			System.out.print('\t');
			System.out.print("minLeafBytes");
			System.out.print('\t');
			System.out.print("maxLeafBytes");
			System.out.print('\n');

			for(Map.Entry<String,PageStats> e : pageStats.entrySet()) {

				final String name = e.getKey();
				
				final PageStats stats = e.getValue();

				final BTree ndx = (BTree) journal.getIndex(name, commitRecord);
				
				System.out.print(name);
				System.out.print('\t');
				System.out.print(ndx.getBranchingFactor());
				System.out.print('\t');
				System.out.print(ndx.getHeight());
				System.out.print('\t');
				System.out.print(ndx.getNodeCount());
				System.out.print('\t');
				System.out.print(ndx.getLeafCount());
				System.out.print('\t');
				System.out.print(stats.nodeBytes);
				System.out.print('\t');
				System.out.print(stats.leafBytes);
				System.out.print('\t');
				System.out.print(stats.getTotalBytes());
				System.out.print('\t');
				System.out.print(stats.getBytesPerNode());
				System.out.print('\t');
				System.out.print(stats.getBytesPerLeaf());
				System.out.print('\t');
				System.out.print(stats.minNodeBytes);
				System.out.print('\t');
				System.out.print(stats.maxNodeBytes);
				System.out.print('\t');
				System.out.print(stats.minLeafBytes);
				System.out.print('\t');
				System.out.print(stats.maxLeafBytes);
				System.out.print('\n');
				
			}

        }
        
    } // dumpNamedIndicesMetadata

}
