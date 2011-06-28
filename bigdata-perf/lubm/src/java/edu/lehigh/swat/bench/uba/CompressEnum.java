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
 * Created on Aug 5, 2009
 */

package edu.lehigh.swat.bench.uba;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Types of compression for the generated files.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum CompressEnum {

    Zip(".zip"),
    GZip(".gz"),
    None("");
    
    private final String ext;
    
    private CompressEnum(String ext) {
        this.ext = ext;
    }
    
    /**
     * The filename extension associated with the compression technique.
     */
    public String getExt() {
        return ext;
    }
    
    /**
     * Return an output stream which imposes the corresponding compression
     * protocol.
     * 
     * @param fileName
     *            The name of the file on which the data will be written.
     *            
     * @return The output stream.
     * 
     * @throws IOException
     */
    public OutputStream getOutputStream(final String fileName)
            throws IOException {
        final OutputStream os;
        switch (this) {
        case GZip:
            os = new GZIPOutputStream(new FileOutputStream(fileName));
            break;
        case Zip:
            os = new DeflaterOutputStream(new FileOutputStream(fileName));
            break;
        case None:
            os = new FileOutputStream(fileName);
            break;
        default:
            throw new AssertionError("Unknown value: " + this);
        }
        return os;
    }
    
}
