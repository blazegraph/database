package com.bigdata.service.mapReduce.tasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.UUID;

import com.bigdata.service.mapReduce.AbstractFileInputMapTask;
import com.bigdata.service.mapReduce.IHashFunction;


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
    public static final String UTF8 = "UTF-8";

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
