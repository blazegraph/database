/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 13, 2011
 */

package com.bigdata.journal;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.CollatorEnum;
import com.bigdata.btree.keys.DecompositionEnum;
import com.bigdata.btree.keys.DefaultKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.IKeyBuilderFactory;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.StrengthEnum;

/**
 * A diagnostic utility for problems with Unicode collation issues which can
 * appear when a journal is moved to another machine. This utility is designed
 * to be run on both the source machine and the target machine. It reports back
 * specific key values from the {@link Name2Addr} index, metadata about the
 * Unicode collation rules in use for that index, and metadata about the
 * {@link Locale} as self-reported by the JVM.  These information are intended
 * for analysis in support of a trouble ticket.
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/193
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JournalMoveDiagnostic {

    private static final Logger log = Logger
            .getLogger(JournalMoveDiagnostic.class);
    
    /**
     * You must specify the name of the Journal file. In addition, you may
     * specify one or more index names. If no index names are specified, it will
     * report metadata for all Name2Addr entries.
     * 
     * @param args
     *            <code>
     * journalFile (indexName)*
     * </code>
     */
    public static void main(final String[] args) {
        
        if (args.length == 0) {

            System.err
                    .println("usage: <filename> (indexName)*");

            System.exit(1);
            
        }

        final File journalFile = new File(args[0]);
        
        if(!journalFile.exists()) {
            
            System.err.println("Not found: "+journalFile);
            
            System.exit(1);
            
        }

        {
 
            System.err.println("Default Locale: " + dumpLocale(Locale.getDefault()));

//            for (Locale tmp : Locale.getAvailableLocales())
//                System.err.println("Available Locale: " + tmp);\
                
        }
        
        // collect the set of index names on which we will report.
        final Set<String> indexNames = new LinkedHashSet<String>();

        for (int i = 1; i < args.length; i++) {

            indexNames.add(args[i]);

        }

        final Properties properties = new Properties();

        {
        
            properties.setProperty(Options.FILE, journalFile.toString());
        
            properties.setProperty(Options.READ_ONLY, "" + true);
            
            // FIXME We should auto-discover this from the root blocks!
            properties.setProperty(Options.BUFFER_MODE,BufferMode.Disk.toString());
        
        }
        
        System.err.println("Opening (read-only): " + journalFile);
        
        final Journal jnl = new Journal(properties);

        try {

            dumpName2Addr(jnl, indexNames, jnl.getLastCommitTime());
            
        } finally {
            
            jnl.shutdownNow();
            
        }
        
    }

    /**
     * Dump out all data associated with the {@link Locale}.
     * 
     * @param l
     *            The {@link Locale}.
     *            
     * @return A string representation of its data.
     */
    private static final String dumpLocale(final Locale l) {
        
        final StringBuilder sb = new StringBuilder();

        sb.append("\n  Locale       : [" + l + "]");
        sb.append("\n  Country      : [" + l.getCountry() + "]");
        sb.append("\n  Language     : [" + l.getLanguage() + "]");
        sb.append("\n  Variant      : [" + l.getVariant() + "]");
        sb.append("\n  ISO3 Country : [" + l.getISO3Country() + "]");
        sb.append("\n  ISO3 Language: [" + l.getISO3Language() + "]");
        sb.append("\n");

        return sb.toString();
        
    }

    /**
     * Dump out some detailed information about the {@link Name2Addr} index, the
     * manner in which it should be encoding Unicode Strings into unsigned
     * byte[] keys, and, for each named index, the actual index name, the actual
     * unsigned byte[] key found in the Name2Addr index, and the unsigned byte[]
     * key under which the machine on which this utility is running would
     * attempt to resolve the index name - this last key SHOULD be the same as
     * the key under which the index entry was found. If it is NOT the same then
     * this indicates an error in the way in which the keys are being generated
     * from the index names.  Information is written onto stderr.
     * 
     * @param jnl
     *            The journal.
     * @param indexNames
     *            The name of one or more indices on which the per-index
     *            metadata will be reported.
     * @param timestamp
     *            The timestamp of the commit record for which this information
     *            will be reported.
     */
    private static final void dumpName2Addr(final Journal jnl,
            final Set<String> indexNames, final long timestamp) {

        final IIndex name2Addr = jnl.getName2Addr(timestamp);

        // The key builder actually used by Name2Addr.
        final IKeyBuilder theKeyBuilder;
        // A key builder from the default factory.
        final IKeyBuilder aKeyBuilder;
        {
            /*
             * Show the key builder factory that the Name2Addr instance is
             * actually using (this shows the tupleSerializer, but that shows
             * the key builder factory which is what we really care about).
             */
            theKeyBuilder = name2Addr.getIndexMetadata().getKeyBuilder();

            /*
             * A key builder factory as it would be configured on this machine
             * for a new Name2Addr index, e.g., if we created a new Journal.
             */
            final IKeyBuilderFactory aKeyBuilderFactory = new DefaultKeyBuilderFactory(
                    new Properties());

            System.err.println("KeyBuilderFactory if created new:\n"
                    + aKeyBuilderFactory);

            /*
             * A key builder generated by that factory. This key builder should
             * have the same behavior that we observe for Name2Addr IF the
             * KeyBuilderFactory inherits the same Locale, [collator],
             * [strength], and [decompositionMode] attributes which were used to
             * create the Journal. Differences in Locale (e.g., language),
             * collator (e.g., JDK versus ICU), strength (e.g., IDENTICAL vs
             * PRIMARY), or decompositionMode (e.g., None versus Full) can all
             * cause the unsigned byte[] keys generated by this key builder to
             * differ from those generated on the machine where (and when) the
             * journal was originally created.
             */
            aKeyBuilder = aKeyBuilderFactory.getKeyBuilder();
            
            System.err.println("Name2Addr effective key builder:\n"
                    + theKeyBuilder);

            System.err.println("Name2Addr if-new    key builder:\n"
                    + aKeyBuilder);
        }

        // Names of indices and the #of times they were found.
        final Map<String, AtomicInteger> dups = new LinkedHashMap<String, AtomicInteger>();

        // the named indices
        final ITupleIterator<?> itr = name2Addr.rangeIterator();

        while (itr.hasNext()) {

            final ITuple<?> tuple = itr.next();

            /*
             * A registered index. Entry.name is the actual name for the index
             * and is serialized using Java default serialization as a String.
             * The key for the entry in the Name2Addr index should be the
             * Unicode sort key for Entry.name. That Unicode sort key should be
             * generated by the collation rules as defined by the IndexMetadata
             * record for the Name2Addr index.
             */
            final Name2Addr.Entry entry = Name2Addr.EntrySerializer.INSTANCE
                    .deserialize(tuple.getValueStream());

            // Track #of times we visit an index having this name.
            {
                
                AtomicInteger tmp = dups.get(entry.name);

                if (tmp == null) {

                    dups.put(entry.name, tmp = new AtomicInteger(0));

                }

                tmp.incrementAndGet();
                
            }
            
            if (!indexNames.isEmpty() && !indexNames.contains(entry.name)) {
                /*
                 * A specific set of index names was given and this is not one
                 * of those indices.
                 */
                continue;
            }
            
            System.err.println("-----");

            System.err.println("Considering: " + tuple);
            
            /*
             * The actual unsigned byte[] under which the Name2Addr entry is
             * indexed.
             */
            final byte[] theKey = tuple.getKey();
            
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
            final byte[] b = name2Addr.getIndexMetadata().getTupleSerializer()
                    .serializeKey(entry.name);
            final byte[] b2 = theKeyBuilder.reset().append(entry.name).getKey();
            if(!BytesUtil.bytesEqual(b, b2)) {
                System.err.println("ERROR: tupleSer and keyBuilder do not agree");
            }
            
//            /*
//             * This uses the key builder which would be created for a new
//             * Name2Addr instance on this host.
//             */
//            final byte[] c = aKeyBuilder.reset().append(entry.name).getKey();
            
            System.err.println("name=" + entry.name);

            System.err.println("tuple : " + BytesUtil.toString(theKey));

            final boolean consistent = BytesUtil.bytesEqual(theKey, b);

//            final boolean consistent2 = BytesUtil.bytesEqual(theKey,c);
            
            if (!consistent) {
                /*
                 * The Name2Addr index has an entry which we will be unable to
                 * locate when given the name of the index because the generated
                 * unsigned byte[] key is NOT the same as the unsigned byte[]
                 * key under which the Entry is stored in the index.
                 */
                System.err.println("recode: " + BytesUtil.toString(b));
                System.err.println("ERROR : Name2Addr inconsistent for ["
                        + entry.name + "]");
                searchForConsistentConfiguration(entry.name, theKey);
            }
//            if (!consistent2) {
//                /*
//                 * @todo javadoc. 
//                 */
//                System.err.println("recod2: " + BytesUtil.toString(c));
//                System.err.println("ERROR : Name2Addr inconsistent for ["
//                        + entry.name + "]");
//            }

        }

        System.err.println("\n===========");
        
        /*
         * Show any indices for which are have more than one entry. There is
         * an encoding problem for the names of any such indices.
         */
        for (Map.Entry<String, AtomicInteger> e : dups.entrySet()) {

            if (e.getValue().get() != 1) {

                System.err.println("ERROR: name=[" + e.getKey() + "] has "
                        + e.getValue().get() + " Name2Addr entries.");

            }
            
        }

    } // dumpName2Addr

    /**
     * Search for a configuration of an {@link IKeyBuilderFactory} which is
     * consistent with the given key when encoding the given string into an
     * unsigned byte[].
     * 
     * @param str
     *            The given string.
     * @param expected
     *            The given key.
     */
    private static void searchForConsistentConfiguration(final String str,
            final byte[] expected) {

//        final byte[] expected = keyBuilder.reset().append(str).getKey();

        // To test all.
        final Locale[] locales = Locale.getAvailableLocales();
        // To test just the default locale.
//        final Locale[] locales = new Locale[]{Locale.getDefault()};
        
        int nconsistent = 0;
        
        // Consider each Locale
        for(Locale l : locales) {
            
            // Consider all Collator implementations (JDK, ICU, ICU4JNI) 
            for(CollatorEnum c : CollatorEnum.values()) {
                
                // Consider all Sollator strengths.
                for(StrengthEnum s : StrengthEnum.values()) {
                    
                    // Consider all Collator decomposition modes.
                    for(DecompositionEnum d : DecompositionEnum.values()) {

                        // Setup the collator.
                        final Properties p = new Properties();
                        p.setProperty(KeyBuilder.Options.USER_COUNTRY, l.getCountry());
                        p.setProperty(KeyBuilder.Options.USER_LANGUAGE, l.getLanguage());
                        p.setProperty(KeyBuilder.Options.USER_VARIANT, l.getVariant());
                        p.setProperty(KeyBuilder.Options.COLLATOR, c.toString());
                        p.setProperty(KeyBuilder.Options.STRENGTH, s.toString());
                        p.setProperty(KeyBuilder.Options.DECOMPOSITION, d.toString());

                        final IKeyBuilderFactory f;
                        final IKeyBuilder tmp;
                        try {
                            f = new DefaultKeyBuilderFactory(p);
                            tmp = f.getKeyBuilder();
                        } catch (IllegalArgumentException t) {
                            if (log.isDebugEnabled())
                                log.debug("Illegal configuration: " + t);
                            continue;
                        } catch (UnsupportedOperationException t) {
                            if (log.isDebugEnabled())
                                log.debug("Illegal configuration: " + t);
                            continue;
                        }

                        final byte[] actual = tmp.reset().append(str).getKey();

                        if (BytesUtil.bytesEqual(expected, actual)) {

                            System.out
                                    .println("Consistent configuration: " + p);

                            nconsistent++;

                        }

                    }

                }

            }

        }

        if (nconsistent == 0) {

            System.err.println("No consistent configuration was found.");
            
        }

    } // searchForConsistentConfiguration()

}
