/*

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
package com.bigdata.service.mapred.tasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.UUID;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.service.mapred.AbstractFileInputMapTask;
import com.bigdata.service.mapred.IHashFunction;

/**
 * Tokenizes an input file, writing <code>{key, term}</code> tuples. The
 * key is an compressed Unicode sort key. The term is a UTF-8 serialization
 * of the term (it can be deserialized to recover the exact Unicode term).
 * 
 * @see CountKeywords
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ExtractKeywords extends AbstractFileInputMapTask {

    /**
     * 
     */
    private static final long serialVersionUID = -6942113098953937858L;
    
    /**
     * The encoding used to serialize the term (the value of each tuple).
     */
    public static final transient String UTF8 = "UTF-8";

    //        /**
    //         * A byte array representing a packed long integer whose value is ONE
    //         * (1L).
    //         */
    //        final byte[] val;

    public ExtractKeywords(UUID uuid, Object source, Integer nreduce,
            IHashFunction hashFunction) {

        super(uuid, source, nreduce, hashFunction);

        //            try {
        //
        //                valBuilder.reset().packLong(1L);
        //
        //            } catch (IOException ex) {
        //
        //                throw new RuntimeException(ex);
        //
        //            }
        //
        //            val = valBuilder.toByteArray();

    }

    public void input(File file, InputStream is) throws Exception {

        // @todo encoding guesser.

        IKeyBuilder keyBuilder = getKeyBuilder();
        
        Reader r = new BufferedReader(new InputStreamReader(is));

        StreamTokenizer tok = new StreamTokenizer(r);

        int nterms = 0;

        boolean done = false;

        while (!done) {

            int ttype = tok.nextToken();

            switch (ttype) {

            case StreamTokenizer.TT_EOF:

                done = true;

                break;

            case StreamTokenizer.TT_NUMBER: {

                double d = tok.nval;

                String s = Double.toString(d);

                keyBuilder.reset().append(s);

                output(s.getBytes(UTF8));

                nterms++;

                break;

            }

            case StreamTokenizer.TT_WORD: {

                String s = tok.sval;

                keyBuilder.reset().append(s);

                output(s.getBytes(UTF8));

                nterms++;

                break;

            }

            }

        }

    }

}
