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
 * Created on Aug 28, 2009
 */

package com.bigdata.btree.raba.codec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;

/**
 * Tokenize an input file, collect the set of distinct keywords, and encode
 * those keywords as unsigned byte[]s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TokenizeKeysGenerator implements IRabaGenerator {

    protected static final Logger log = Logger
            .getLogger(TokenizeKeysGenerator.class);
    
    /**
     * The encoding used to serialize the term (the value of each tuple).
     */
    public static final transient String charset = "UTF-8";

    public TokenizeKeysGenerator(String fileOrResource) {
        
        final Reader r;
        if(new File(fileOrResource).exists()) {
        
            try {
                r = new BufferedReader(new FileReader(fileOrResource));
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not open file: "
                        + fileOrResource);
            }
            
        } else {
            
            final InputStream is = getClass().getResourceAsStream(
                    fileOrResource);

            if (is == null) {

                throw new RuntimeException("No such resource: "+fileOrResource);
                
            }
            
            r = new BufferedReader(new InputStreamReader(is));
            
        }
        
        // tokenize.
        final Set<String> tokens;
        try {
            tokens = tokenize(fileOrResource, r);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
        // encode.
        data = new byte[tokens.size()][];
        int i = 0;
        for(String s : tokens) {
            try {
                data[i++] = s.getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Could not encode: " + s
                        + ", charset=" + charset + " : " + e, e);
            }
        }
        
    }
    final byte[][] data;
    
    // @todo bias is for the keys which are earliest in the lexical order.
    public byte[][] generateKeys(final int size) {

        // clone so we don't disturb the order when we sort the data.
        final byte[][] a = data.clone();

        // Place the keys into sorted order.
        Arrays.sort(a, BytesUtil.UnsignedByteArrayComparator.INSTANCE);

        // clear unused keys.
        for (int i = size; i < a.length; i++) {

            a[i] = null;

        }
        
        return a;
        
    }

    public byte[][] generateValues(final int size) {

        // clone so we don't disturb the order when we clear unused entries.
        final byte[][] a = data.clone();

        // @todo could also set some percentage of the values to null.
        
        // clear unused values.
        for (int i = size; i < a.length; i++) {

            a[i] = null;

        }
        
        return a;
        
    }

    /**
     * Yes.
     */
    public boolean isKeysGenerator() {
        return true;
    }

    /**
     * Yes.
     */
    public boolean isValuesGenerator() {
        return true;
    }

//    Reader r = new BufferedReader(new InputStreamReader(is));

    public Set<String> tokenize(final String fileOrResource, final Reader r)
            throws Exception {

        // the distinct terms.
        final Set<String> terms = new HashSet<String>(10000);

        // the tokenizer.
        final StreamTokenizer tok = new StreamTokenizer(r);

        // #of tokens processed.
        int count = 0;

        boolean done = false;

        while (!done) {

            final int ttype = tok.nextToken();

            switch (ttype) {

            case StreamTokenizer.TT_EOF:

                done = true;

                break;

            case StreamTokenizer.TT_NUMBER: {

                double d = tok.nval;

                String s = Double.toString(d);

                terms.add(s);

                count++;

                break;

            }

            case StreamTokenizer.TT_WORD: {

                final String s = tok.sval;

                terms.add(s);

                count++;

                break;

            }

            }

        }

        if (log.isInfoEnabled()) {
            log.info("Tokenized: " + count + " tokens with " + terms.size()
                    + " distinct terms : src=" + fileOrResource);
        }

        return terms;

    }

}
