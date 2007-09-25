package com.bigdata.service.mapReduce.tasks;

import java.io.File;
import java.io.InputStream;
import java.util.UUID;

import com.bigdata.service.mapReduce.AbstractFileInputMapTask;
import com.bigdata.service.mapReduce.IHashFunction;

/**
 * Reads the bytes and throws them away.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyMapTask extends AbstractFileInputMapTask {

    /**
     * 
     */
    private static final long serialVersionUID = 2055918237931155126L;

    public ReadOnlyMapTask(UUID uuid, Object source, Integer nreduce, IHashFunction hashFunction) {

        super(uuid, source, nreduce, hashFunction);

    }
    
    public void input(File file, InputStream is) throws Exception {

        while(true) {
            
            int ch = is.read();
            
            if(ch==-1) break;
            
        }
        
    }

}