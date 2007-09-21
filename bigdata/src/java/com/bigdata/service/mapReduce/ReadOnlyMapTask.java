package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.InputStream;
import java.util.UUID;

/**
 * Reads the bytes and throws them away.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyMapTask extends AbstractFileInputMapTask {

    public ReadOnlyMapTask(UUID uuid, Integer nreduce, IHashFunction hashFunction) {

        super(uuid, nreduce, hashFunction);

    }
    
    public void input(File file, InputStream is) throws Exception {

        while(true) {
            
            int ch = is.read();
            
            if(ch==-1) break;
            
        }
        
    }

}