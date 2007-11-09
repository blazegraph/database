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
 * Created on Oct 31, 2006
 */

package com.bigdata.journal;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * Type safe enumeration of options governing whether and how a file is forced
 * to stable storage. This enum is used for a variety of behaviors, including
 * commit semantics and the mode in which the {@link RandomAccessFile} is
 * opened.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum ForceEnum {

    /**
     * The file is not forced to stable storage.
     */
    No("No"),
    
    /**
     * The file data, but NOT the file metadata, are forced to stable storage.
     * 
     * @see FileChannel#force(boolean)
     */
    Force("Force"),
    
    /**
     * The file data and the file metadata are forced to stable storage.
     * 
     * @see FileChannel#force(boolean)
     */
    ForceMetadata("ForceMetadata");
    
    private String name;
    
    private ForceEnum(String name) {
        this.name = name;
    }

    public String toString() {
        
        return name;
        
    }

    /**
     * Parse a string whose contents must be "No", "Force", "ForceMetadata".
     * 
     * @param s
     *            The string.
     * 
     * @return The named {@link ForceEnum}.
     */
    public static ForceEnum parse(String s) {
        if( s == null ) throw new IllegalArgumentException();
        if( s.equals(No.name)) return No;
        if( s.equals(Force.name)) return Force;
        if( s.equals(ForceMetadata.name)) return ForceMetadata;
        throw new IllegalArgumentException("Unknown value: "+s);
    }

    /**
     * Return the read-write file mode corresponding to the enum value.
     *  
     * @see RandomAccessFile#RandomAccessFile(java.io.File, String)
     * 
     * @todo Write unit test.
     */
    public String asFileMode() {

        switch (this) {
        case No:
            return "rw";
        case Force:
            // force data only.
            return "rwd";
        case ForceMetadata:
            // force data + metadata.
            return "rws";
        default:
            throw new AssertionError();
        }

    }

}
