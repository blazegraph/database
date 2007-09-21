package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.InputStream;
import java.util.UUID;

/**
 * Does nothing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPMapTask extends AbstractFileInputMapTask {

    public NOPMapTask(UUID uuid, Integer nreduce, IHashFunction hashFunction) {

        super(uuid, nreduce, hashFunction);

    }
    
    public void input(File file, InputStream is) throws Exception {

    }

}