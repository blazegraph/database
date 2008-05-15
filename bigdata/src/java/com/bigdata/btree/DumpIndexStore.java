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
 * Created on May 15, 2008
 */

package com.bigdata.btree;

import java.io.File;
import java.util.Properties;

import com.bigdata.journal.DumpJournal;
import com.bigdata.rawstore.IRawStore;

/**
 * Utility to examine the context of an {@link IndexSegmentStore}.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DumpIndexStore {

    public static void usage() {
     
        System.err.println("usage: " + DumpIndexStore.class.getSimpleName()
                + "[options] " + " file(s)");
        
//        System.err.println("options:");
//
//        System.err.println(" -i: dump Index");
        
    }
    
    public static void main(String[] args) {
        
        if(args.length==0) {
         
            usage();
            
            System.exit(1);
            
        }

        for (String fileStr : args) {

            File file = new File(fileStr);

            if (!file.exists()) {

                System.err.println("No such file: " + fileStr);

                System.exit(1);

            }

            Properties properties = new Properties();

            properties.setProperty(IndexSegmentStore.Options.SEGMENT_FILE,
                    fileStr);

            IndexSegmentStore store = new IndexSegmentStore(properties);

            // dump the checkpoint record, index metadata record, etc.
            dump(store);
        
            // dump the index contents : @todo command line option.
            if(true) DumpJournal.dumpIndex(store.loadIndexSegment());
            
        }

    }

    static void dump(IndexSegmentStore store) {

        System.err.println("file        : " + store.getFile());

        System.err.println("checkpoint  : " + store.getCheckpoint().toString());

        System.err.println("metadata    : " + store.getIndexMetadata().toString());
        
        System.err.println("bloomFilter : "
                + (store.getCheckpoint().addrBloom != IRawStore.NULL ? store
                        .getBloomFilter().toString() : "N/A"));
        
    }
    
}
