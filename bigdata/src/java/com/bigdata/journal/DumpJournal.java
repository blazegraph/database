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
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.DumpIndex;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.ISimpleTreeIndexAccess;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.PageStats;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rwstore.RWStore;
import com.bigdata.util.ChecksumUtility;
import com.bigdata.util.InnerCause;

/**
 * A utility class that opens the journal in a read-only mode and dumps the root
 * blocks and metadata about the indices on a journal file.
 * 
 * TODO add an option to dump only as of a specified commitTime?
 * 
 * TODO add an option to restrict the names of the indices to be dumped
 * (-name=<regex>).
 * 
 * TODO GIST : Support all types of indices.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
 *      </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DumpJournal {

    private static final Logger log = Logger.getLogger(DumpJournal.class);
    
//    public DumpJournal() {
//        
//    }
    
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
    public static void main(final String[] args) {
        
		if (args.length == 0) {
            
            System.err.println("usage: (-history|-indices|-pages|-tuples) <filename>+");
            
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

                    properties.setProperty(Options.BUFFER_MODE,
                            BufferMode.Disk.toString());

                }

                System.out.println("Opening (read-only): " + file);

                final Journal journal = new Journal(properties);

                try {

                    final DumpJournal dumpJournal = new DumpJournal(journal);

                    dumpJournal.dumpJournal(dumpHistory, dumpPages,
                            dumpIndices, showTuples);

                } finally {
                    
                    journal.close();
                    
                }

            } catch( RuntimeException ex) {
                
                ex.printStackTrace();

                System.err.println("Error: "+ex+" on file: "+file);
                
            }
            
            System.out.println("==================================");

        }
        
    }
    
    /**
     * 
     * @param dumpHistory
     *            Dump metadata for indices in all commit records (default only
     *            dumps the metadata for the indices as of the most current
     *            committed state).
     * @param dumpPages
     *            Dump the pages of the indices and reports some information on
     *            the page size.
     * @param dumpIndices
     *            Dump the indices (does not show the tuples by default).
     * @param showTuples
     *            Dump the records in the indices.
     */
    public void dumpJournal(final boolean dumpHistory, final boolean dumpPages,
            final boolean dumpIndices, final boolean showTuples) {

        final FileMetadata fmd = journal.getFileMetadata();

        if (fmd != null) {
            
            /*
             * Note: The FileMetadata is only available on a re-open of an
             * existing Journal.
             */

            // dump the MAGIC and VERSION.
            System.out.println("magic=" + Integer.toHexString(fmd.magic));
            System.out.println("version="
                    + Integer.toHexString(fmd.version));

            /*
             * Report on:
             * 
             * - the length of the journal. - the #of bytes available for
             * user data in the journal. - the offset at which the next
             * record would be written. - the #of bytes remaining in the
             * user extent.
             */

            final long bytesAvailable = (fmd.userExtent - fmd.nextOffset);

            System.out.println("extent=" + fmd.extent + "(" + fmd.extent
                    / Bytes.megabyte + "M)" + ", userExtent="
                    + fmd.userExtent + "(" + fmd.userExtent
                    / Bytes.megabyte + "M)" + ", bytesAvailable="
                    + bytesAvailable + "(" + bytesAvailable
                    / Bytes.megabyte + "M)" + ", nextOffset="
                    + fmd.nextOffset);

        }

        {

            /*
             * Dump the root blocks.
             * 
             * Note: This uses the IBufferStrategy to access the root
             * blocks. The code used to use the FileMetadata, but that was
             * only available for a re-opened journal. This approach works
             * for a new Journal as well.
             */
            {

                final ByteBuffer rootBlock0 = journal.getBufferStrategy()
                        .readRootBlock(true/* rootBlock0 */);
                
                if (rootBlock0 != null) {
                
                    System.out.println(new RootBlockView(
                            true/* rootBlock0 */, rootBlock0,
                            new ChecksumUtility()).toString());
                    
                }
                
            }

            {

                final ByteBuffer rootBlock1 = journal.getBufferStrategy()
                        .readRootBlock(false/* rootBlock0 */);
                
                if (rootBlock1 != null) {
                
                    System.out.println(new RootBlockView(
                            false/* rootBlock0 */, rootBlock1,
                            new ChecksumUtility()).toString());
                    
                }
                
            }
            // System.out.println(fmd.rootBlock0.toString());
            // System.out.println(fmd.rootBlock1.toString());

            // report on which root block is the current root block.
            System.out.println("The current root block is #"
                    + (journal.getRootBlockView().isRootBlock0() ? 0 : 1));

        }

        final IBufferStrategy strategy = journal.getBufferStrategy();

        if (strategy instanceof RWStrategy) {

            final RWStore store = ((RWStrategy) strategy).getStore();

            final StringBuilder sb = new StringBuilder();

            store.showAllocators(sb);

            System.out.println(sb);

        }
        
		final CommitRecordIndex commitRecordIndex = journal
				.getCommitRecordIndex();

		System.out.println("There are " + commitRecordIndex.getEntryCount()
				+ " commit points.");

		if (dumpHistory) {

            System.out.println("Historical commit points follow in temporal sequence (first to last):");
            
//                final IKeyBuilder keyBuilder = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);
//
//				final long targetTime = 1303505388420L;
//				int indexOf = commitRecordIndex.indexOf(keyBuilder.reset()
//						.append(targetTime).getKey());
//				if (indexOf < 0)
//					indexOf = (-(indexOf) - 1);
//
//				// @todo handle leading/trailing edge cases.
//                final long fromTime = KeyBuilder.decodeLong(commitRecordIndex.keyAt(indexOf-3), 0/*off*/);
//                final long toTime = KeyBuilder.decodeLong(commitRecordIndex.keyAt(indexOf+3), 0/*off*/);
//                
//                final ITupleIterator<CommitRecordIndex.Entry> itr = commitRecordIndex.rangeIterator(
//                		keyBuilder.reset().append(fromTime+1).getKey(),
//                		keyBuilder.reset().append(toTime+1).getKey()
//                		);

            @SuppressWarnings("unchecked")
            final ITupleIterator<CommitRecordIndex.Entry> itr = commitRecordIndex.rangeIterator();
            
            while(itr.hasNext()) {
                
                System.out.println("----");

                final CommitRecordIndex.Entry entry = itr.next().getObject();
                
                System.out.print("Commit Record: " + entry.commitTime
                        + ", addr=" + journal.toString(entry.addr)+", ");
                
                final ICommitRecord commitRecord = journal
                        .getCommitRecord(entry.commitTime);
                
                System.out.println(commitRecord.toString());

                dumpNamedIndicesMetadata(commitRecord, dumpPages,
                        dumpIndices, showTuples);

            }
            
        } else {

            /*
             * Dump the current commit record.
             */
            
            final ICommitRecord commitRecord = journal.getCommitRecord();
            
            System.out.println(commitRecord.toString());

            dumpNamedIndicesMetadata(commitRecord, dumpPages, dumpIndices,
                    showTuples);
            
        }

    }

    private final Journal journal;

    public DumpJournal(final Journal journal) {

        if (journal == null)
            throw new IllegalArgumentException();
        
        this.journal = journal;

    }
    
    /**
     * Dump metadata about each named index as of the specified commit record.
     * 
     * @param journal
     * @param commitRecord
     */
    private void dumpNamedIndicesMetadata(final ICommitRecord commitRecord,
            final boolean dumpPages, final boolean dumpIndices,
            final boolean showTuples) {

        final Iterator<String> nitr = journal.indexNameScan(null/* prefix */,
                commitRecord.getTimestamp());

        final Map<String, PageStats> pageStats = dumpPages ? new TreeMap<String, PageStats>()
                : null;
		
        while (nitr.hasNext()) {

            // a registered index.
            final String name = nitr.next();
            
            System.out.println("name=" + name);

            // load index from its checkpoint record.
            final ICheckpointProtocol ndx;
            try {

                ndx = journal.getIndexWithCommitRecord(name, commitRecord);

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

            /*
             * Collect statistics on the page usage for the index.
             * 
             * TODO If we kept the BTree counters for the #of bytes written per
             * node and per leaf up to date when nodes and leaves were recycled
             * then we could generate (parts of) this table very quickly. As it
             * stands, we have to actually scan the pages in the index.
             */
            if (ndx instanceof ISimpleTreeIndexAccess) {

                if (pageStats != null) {

                    final PageStats stats = ((ISimpleTreeIndexAccess) ndx)
                            .dumpPages();

                    System.out.println("\t" + stats);

                    pageStats.put(name, stats);

                }

                if (dumpIndices) {

                    if (ndx instanceof AbstractBTree) {
                    
                        /*
                         * TODO GIST : dumpTuples for HTree.
                         */
                        
                        DumpIndex.dumpIndex((AbstractBTree) ndx, showTuples);
                        
                    }

                }

            }

        }

        if (pageStats != null) {

            /*
             * Write out the header.
             */
            System.out.println(PageStats.getHeaderRow());

            for (Map.Entry<String, PageStats> e : pageStats.entrySet()) {

                final String name = e.getKey();

                final PageStats stats = e.getValue();

                if (stats == null) {

                    /*
                     * Something for which we did not extract the PageStats.
                     */

                    final ICheckpointProtocol tmp = journal
                            .getIndexWithCommitRecord(name, commitRecord);

                    System.out.println("name: " + name + ", class="
                            + tmp.getClass() + ", checkpoint="
                            + tmp.getCheckpoint());

                    continue;
                    
                }

                /*
                 * Write out the stats for this index.
                 */

                System.out.println(stats.getDataRow());

			}

        }
        
    } // dumpNamedIndicesMetadata

}
