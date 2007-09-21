package com.bigdata.service.mapReduce;

import java.util.Iterator;
import java.util.UUID;

/**
 * Summarizes tuples of the form <code>{key, term}</code>.
 * <p>
 * Note that many terms may be conflated into the same Unicode sort key
 * depending on the collator that you are using. This task just deserializes
 * the 1st term entry for each distinct key. If you want some consistency in
 * the reported terms, then you should normalize the terms in your map task.
 * 
 * @see ExtractKeywords
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CountKeywords extends AbstractReduceTask {

    public CountKeywords(UUID uuid, UUID dataService) {
        
        super(uuid, dataService );
        
    }
    
    public void reduce(byte[] key, Iterator<byte[]> vals) throws Exception {

        String term = null;

        boolean first = true;

        long count = 0L;

        while (vals.hasNext()) {

            byte[] val = vals.next();

            if (first) {

                term = new String(val, ExtractKeywords.UTF8);

                first = false;

            }
            
            count++;

        }

        if (count == 0)
            throw new AssertionError();
        
        System.err.println(term+" : "+count);
        
    }

}