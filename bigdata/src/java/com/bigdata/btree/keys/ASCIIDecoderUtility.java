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
 * Created on Apr 22, 2009
 */

package com.bigdata.btree.keys;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Utility reads unsigned byte[] keys from stdin and writes their decoded ASCII
 * values on stdout. This may be used to decode the keys for an index whose keys
 * are encoded ASCII strings. It CAN NOT be used to decode Unicode sort keys.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASCIIDecoderUtility {

    /**
     * 
     */
    public ASCIIDecoderUtility() {
    }

    /**
     * Reads decimal representations of unsigned bytes representing ASCII
     * characters from stdin, writing their ASCII values on stdout. The
     * following characters are stripped from the input: <code>[]</code>. The
     * values may be delimited by commas or whitespace or both.
     * 
     * @param args
     *            ignored.
     * 
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        final BufferedReader r = new BufferedReader(new InputStreamReader(
                System.in));
        
        String s;
        while ((s = r.readLine()) != null) {

            final String[] a = s.split("[\\s,\\[\\]]+");

            final byte[] b = new byte[a.length];
            
            int len = 0;
            for (String t : a) {

                if (t.length() == 0)
                    continue;

                final int i = Integer.parseInt(t);

                b[len++] = (byte) i;

            }

            final String ascii = KeyBuilder
                    .decodeASCII(b, 0/*off*/, len/*len*/);

            System.out.println(ascii);

        }

    }

}
