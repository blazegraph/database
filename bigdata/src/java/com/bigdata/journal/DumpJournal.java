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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.relation.IDatabase;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationSchema;
import com.bigdata.sparse.GlobalRowStoreHelper;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.util.InnerCause;

/**
 * A utility class that opens the journal in a read-only mode and dumps the root
 * blocks and metadata about the indices on a journal file.
 * 
 * @todo add an option to collect histograms over index records so that "fat" in
 *       the indices may be targeted. We can always report histograms for the
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

    private static final Logger log = Logger.getLogger(DumpJournal.class);
    
//    protected static final boolean INFO = log.isInfoEnabled();
    
    public DumpJournal() {
        
    }

    /**
     * Dump one or more journal files.
     * 
     * @param args
     *            The name(s) of the journal file to open.
     *            <dl>
     *            <dt>-history</dt>
     *            <dd>Dump metadata for indices in all commit records (default
     *            only dumps the metadata for the indices as of the most current
     *            committed state).</dd>
     *            <dt>-GRS</dt>
     *            <dd>Dump the records in the global row store (this contains
     *            the various {@link IDatabase} and {@link IRelation}
     *            declarations).</dd>
     *            <dt>-GRSAll</dt>
     *            <dd>Dump the records in the global row store (this contains
     *            the various {@link IDatabase} and {@link IRelation}
     *            declarations). For each property in the global row store, show
     *            all property value updates together with their timestamps.</dd>
     *            <dt>-indices</dt>
     *            <dd>Dump the indices (does not show the tuples by default).</dd>
     *            <dt>-tuples</dt>
     *            <dd>Dump the records in the indices.</dd>
     *            </dl>
     */
    public static void main(final String[] args) {

        if (args.length == 0) {

            System.err
                    .println("usage: (-history|-GRS|-GRSAll|-indices|-tuples) <filename>+");

            System.exit(1);
            
        }

        int i = 0;

        boolean dumpGRS = false;

        boolean dumpGRSCurrentOnly = true;

        boolean dumpHistory = false;
        
        boolean dumpIndices = false;
        
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

            if(arg.equals("-GRS")) {
                
                dumpGRS = true;
                
            }

            if(arg.equals("-GRSAll")) {
                
                dumpGRS = true;

                dumpGRSCurrentOnly = false;
                
            }

            if(arg.equals("-indices")) {
                
                dumpIndices = true;
                
            }

            if(arg.equals("-tuples")) {
                
                showTuples = true;
                
            }
            
        }

        for (; i < args.length; i++) {

            final File file = new File(args[i]);

            try {

                dumpJournal(file, dumpHistory, dumpGRS, dumpGRSCurrentOnly,
                        dumpIndices, showTuples);

            } catch (RuntimeException ex) {

                ex.printStackTrace();

                System.err.println("Error: " + ex + " on file: " + file);

            }

            System.err.println("==================================");

        }
        
    }
    
    public static void dumpJournal(final File file, final boolean dumpHistory,
            final boolean dumpGRS, final boolean dumpGRSCurrentOnly,
            final boolean dumpIndices, final boolean showTuples) {
        
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
            
            // FIXME We should auto-discover this from the root blocks!
            properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
        }
        
        System.err.println("Opening (read-only): "+file);
        
        final Journal journal = new Journal(properties);

        try {
            
            final FileMetadata fmd = journal.fileMetadata;

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
                
                final CommitRecordIndex commitRecordIndex = journal.getCommitRecordIndex();
//                CommitRecordIndex commitRecordIndex = journal._commitRecordIndex;
                
                final ITupleIterator<CommitRecordIndex.Entry> itr = commitRecordIndex.rangeIterator();
                
                while(itr.hasNext()) {
                    
                    System.err.println("----");

                    final CommitRecordIndex.Entry entry = itr.next().getObject();
                    
                    System.err.print("Commit Record: " + entry.commitTime
                            + ", addr=" + journal.toString(entry.addr)+", ");
                    
                    final ICommitRecord commitRecord = journal
                            .getCommitRecord(entry.commitTime);

                    System.err.println(commitRecord.toString());

                    dumpNamedIndicesMetadata(journal, commitRecord
                            .getTimestamp(), dumpGRS, dumpGRSCurrentOnly,
                            dumpIndices, showTuples);

                }

            } else {

                /*
                 * Dump the current commit record.
                 */

                final ICommitRecord commitRecord = journal.getCommitRecord();

                System.err.println(commitRecord.toString());

                dumpNamedIndicesMetadata(journal, commitRecord.getTimestamp(),
                        dumpGRS, dumpGRSCurrentOnly, dumpIndices, showTuples);

            }

        } finally {

            journal.close();

        }

    }
    
    /**
     * Dump metadata about each named index as of the specified commit record.
     * 
     * @param journal
     *
     */
    private static void dumpNamedIndicesMetadata(final AbstractJournal journal,
            final long timestamp, final boolean dumpGRS,
            final boolean dumpGRSCurrentOnly, final boolean dumpIndices,
            final boolean showTuples) {

        // view as of that commit record.
        final IIndex name2Addr = journal.getName2Addr(timestamp);

        if (dumpGRS) {
         
            // Look up the GRS as of that timestamp.
            final SparseRowStore rowStore = new GlobalRowStoreHelper(journal)
                    .get(timestamp);
            
            if (rowStore == null) {
            
                System.err.println("GlobalRowStore does not exist: timestamp="
                        + timestamp);
                
            } else {

//                dumpRowStore(rowStore, GlobalRowStoreSchema.INSTANCE,
//                        dumpGRSCurrentOnly);
                
                dumpRowStore(rowStore, RelationSchema.INSTANCE,
                        dumpGRSCurrentOnly);

            }
            
        }

        System.err.println(name2Addr.getIndexMetadata().getTupleSerializer().toString());
        
        // the named indices
        final ITupleIterator<?> itr = name2Addr.rangeIterator();

        while (itr.hasNext()) {

            final ITuple<?> tuple = itr.next();

            /*
             * A registered index. Entry.name is the actual name for the
             * index and is serialized using Java default serialization as a
             * String. The key for the entry in the Name2Addr index should
             * be the Unicode sort key for Entry.name. That Unicode sort key
             * should be generated by the collation rules as defined by the
             * IndexMetadata record for the Name2Addr index. 
             */
            final Name2Addr.Entry entry = Name2Addr.EntrySerializer.INSTANCE
                    .deserialize(tuple.getValueStream());

            /*
             * Using the TupleSerializer for the Name2Addr index, generate the
             * Unicode sort key for Entry.name. This *should* be the same as the
             * unsigned byte[] key for the tuple in the Name2Addr index. If it
             * is NOT the same, then there is a problem with the preservation of
             * the Unicode collation rules such that the same input string
             * (Entry.name) is resulting in a different unsigned byte[] key. If
             * this happens, then the indices can appear to become "lost"
             * because the "spelling rules" for the Name2Addr index have
             * changed.
             * 
             * @see https://sourceforge.net/apps/trac/bigdata/ticket/193
             */
            final byte[] b = name2Addr.getIndexMetadata().getTupleSerializer().serializeKey(entry.name);
            
            System.err.println("name=" + entry.name + ", addr="
                    + journal.toString(entry.checkpointAddr));

            if(!BytesUtil.bytesEqual(b, tuple.getKey())) {
                /*
                 * The Name2Addr index has an entry which we will be unable to
                 * locate when given the name of the index because the generated
                 * unsigned byte[] key is NOT the same as the unsigned byte[]
                 * key under which the Entry is stored in the index.
                 */
                System.err.println("ERROR : Name2Addr inconsistent: Entry.name="+entry.name);
                System.err.println("tuple : "+BytesUtil.toString(tuple.getKey()));
                System.err.println("recode: "+BytesUtil.toString(b));
                System.err.println("-----");
            }
            
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
            System.err.println("\t" + ndx.getCheckpoint());

            // show metadata record.
            System.err.println("\t" + ndx.getIndexMetadata());

            if (dumpIndices)
                dumpIndex(ndx, showTuples);

        }
        
    }

    /**
     * Dump the contents of a row store, typically this will be the global row
     * store, but it can be used for any row store.
     * 
     * @param rowStore
     *            The row store to be dumped.
     * @param schema
     *            The schema to be dumped. Other schemas will be ignored (the
     *            schema name serves as a key prefix such that different schemas
     *            occupy disjoint key-ranges in the row store).
     * @param currentOnly
     *            When <code>true</code>, only the value having the most current
     *            timestamp for each property will be dumped. When
     *            <code>false</code>, all values for each property will be
     *            dumped as {property,timestamp,value} tuples.
     */
    private static void dumpRowStore(final SparseRowStore rowStore,
            final Schema schema, final boolean currentOnly) {

        if (rowStore == null)
            throw new IllegalArgumentException();

        if (schema == null)
            throw new IllegalArgumentException();

        final Iterator<? extends ITPS> itr = rowStore.rangeIterator(schema);

        while (itr.hasNext()) {

            final ITPS tps = itr.next();

            System.err.println(tps.getSchema().getName() + "::"
                    + tps.getPrimaryKey());
            
            if (currentOnly) {

                // visit in sorted order.
                final Iterator<Map.Entry<String, Object>> eitr = new TreeMap<String, Object>(
                        tps.asMap()).entrySet().iterator();

                while (eitr.hasNext()) {

                    final Map.Entry<String, Object> e = eitr.next();

                    System.err.println("\t" + e.getKey() + "=" + e.getValue());

                }

            } else {

                // visit in order by ascending timestamp.
                final Iterator<ITPV> eitr = tps.iterator();

                while (eitr.hasNext()) {

                    final ITPV tpv = eitr.next();

                    System.err.println("\t" + tpv.getName() + "[@"
                            + tpv.getTimestamp() + "]" + "=" + tpv.getValue());

                }
                
            }
            
        }

    }
    
    /**
     * Utility method using an {@link ITupleIterator} to dump the keys and
     * values in an {@link AbstractBTree}.
     * 
     * @param btree
     * @param showTuples
     *            When <code>true</code> the data for the keys and values will
     *            be displayed. Otherwise the scan will simply exercise the
     *            iterator.
     */
    public static void dumpIndex(final AbstractBTree btree,
            final boolean showTuples) {

        // @todo offer the version metadata also if the index supports isolation.
        final ITupleIterator<?> itr = btree.rangeIterator();
        
        final long begin = System.currentTimeMillis();
        
        int i = 0;
        
        while(itr.hasNext()) {
            
            final ITuple<?> tuple = itr.next();

            if(showTuples) {

                System.err.println("rec="+i+dumpTuple( tuple ));
                
            }
            
            i++;
            
        }
        
        final long elapsed = System.currentTimeMillis() - begin;
        
        System.err.println("Visited "+i+" tuples in "+elapsed+"ms");
        
    }
    
    private static String dumpTuple(ITuple<?> tuple) {
        
        final ITupleSerializer<?, ?> tupleSer = tuple.getTupleSerializer();
        
        final StringBuilder sb = new StringBuilder();
        
        try {

            sb.append("\nkey="+tupleSer.deserializeKey(tuple));
            
        } catch(Throwable t) {
            
            sb.append("\nkey="+BytesUtil.toString(tuple.getKey()));
            
        }

        try {

            sb.append("\nval="+tupleSer.deserialize(tuple));
            
        } catch(Throwable t) {

            sb.append("\nval="+BytesUtil.toString(tuple.getValue()));
            
        }

        return sb.toString();
        
    }
    
}
