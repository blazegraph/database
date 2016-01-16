/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Feb 10, 2012
 */

package com.bigdata.journal;

import java.io.File;
import java.util.Properties;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.rwstore.RWStore;

/**
 * Given an existing journal, ensure that any commitRecords that reference a
 * time prior to the last deferred release time are removed. This provides a
 * "fix" for opening a bigdata 1.0.4 journal in bigdata 1.0.6.  
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/480"> Error
 *      releasing deferred frees using 1.0.6 against a 1.0.4 journal</a>
 *      
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn Cutcher</a>
 * @version $Id$
 */
public class VerifyCommitRecordIndex {
    
    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: <journal filename>");
            
            return;
        }
        
        final String fname = args[0];
        final File f = new File(fname);
        if (!f.exists()) {
            System.err.println("File: " + fname + " not found");
            
            return;
        }
        
        final Properties props = new Properties();
        props.setProperty(Options.FILE, fname);
        
        final Journal jrnl = new Journal(props);
        
        // Check if journal is DISKRW
        if (jrnl.getBufferStrategy().getBufferMode() != BufferMode.DiskRW) {            
            System.err.println("Buffer mode should be DiskRW not " + jrnl.getBufferStrategy().getBufferMode());
            
            return;
        }
        
        final RWStrategy rwstrategy = (RWStrategy) jrnl.getBufferStrategy();
        final RWStore rwstore = rwstrategy.getStore();
        
        final IIndex commitRecordIndex = jrnl.getReadOnlyCommitRecordIndex();
        if (commitRecordIndex == null) {
            System.err.println("Unexpected null commit record index");
            return;
        }

        final IndexMetadata metadata = commitRecordIndex
                .getIndexMetadata();

        final byte[] zeroKey = metadata.getTupleSerializer()
                .serializeKey(0L);

        final byte[] releaseKey = metadata.getTupleSerializer()
                .serializeKey( rwstore.getLastReleaseTime());

        final int removed = jrnl.removeCommitRecordEntries(zeroKey, releaseKey);
        
        System.out.println("Commit Record Index verified with " + removed + " records removed");

        jrnl.commit();
        
    }

}
