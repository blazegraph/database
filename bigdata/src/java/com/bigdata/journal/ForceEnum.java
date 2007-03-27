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
