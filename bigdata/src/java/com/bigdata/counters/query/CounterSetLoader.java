/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 6, 2009
 */

package com.bigdata.counters.query;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.store.CounterSetBTree;
import com.bigdata.journal.Journal;

/**
 * Utility class to load data from XML representations of counter sets into a
 * {@link CounterSetBTree} on a {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CounterSetLoader {

    /**
     * Reads counters from XML files into a Journal.
     * 
     * @param args
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws IOException
     */
    static public void main(String[] args) throws IOException,
            ParserConfigurationException, SAXException {
    
        final Properties properties = new Properties();
    
        // @todo config
        properties.setProperty(Journal.Options.FILE,"counters.jnl");
        
        // (re-)open the store.
        final Journal store = new Journal(properties);
    
        // (re-)open/create the counter set B+Tree on the store.
        CounterSetBTree btree = (CounterSetBTree) store.getIndex("counters");
    
        if (btree == null) {
    
            // not registered, so create and register it now.
            btree = CounterSetBTree.create(store);
    
            store.registerIndex("counters", btree);
    
            // commit the registered index.
            store.commit();
            
        }
        
        /*
         * Process each file, loading data into the counter set B+Tree. 
         */
        for (String s : args) {
    
            final File file = new File(s);

            loadFile(btree, file);
            
        } // next source file.

        System.err.println("There are " + btree.rangeCount()
                + " counter values covering "
                + new Date(btree.getFirstTimestamp()) + " to "
                + new Date(btree.getLastTimestamp()));

    } // main

    static private void loadFile(final CounterSetBTree btree, final File file)
            throws IOException, ParserConfigurationException, SAXException {

        if (file.isDirectory()) {

            final File[] files = file.listFiles();

            for (File f : files) {

                if (f.isFile())
                    loadFile(btree, f);

            }

            return;

        }

        final CounterSet counterSet = new CounterSet();

        System.out.println("reading file: " + file);

        InputStream is = null;

        try {

            is = new BufferedInputStream(new FileInputStream(file));

            /*
             * Note: This will throw a runtime exception if a source file
             * contains more than 60 minutes worth of history data (there should
             * only be 60 minutes in the LBS generated dumps).
             */
            counterSet.readXML(is, DefaultInstrumentFactory.NO_OVERWRITE_60M,
                    null/* filter */);

        } finally {

            if (is != null) {

                is.close();

            }

        }

        System.out.println("Writing counters on store.");

        btree.writeHistory(counterSet.getCounters(null/*filter*/));

        // commit after each file loaded (easier to restart).
        ((Journal)btree.getStore()).commit();
        
    }

}
