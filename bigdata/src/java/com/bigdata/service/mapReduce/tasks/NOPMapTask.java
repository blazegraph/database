package com.bigdata.service.mapReduce.tasks;

import java.io.File;
import java.io.InputStream;
import java.util.UUID;

import com.bigdata.service.mapReduce.AbstractFileInputMapTask;
import com.bigdata.service.mapReduce.IHashFunction;

/**
 * Does nothing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NOPMapTask extends AbstractFileInputMapTask {

    /**
     * 
     */
    private static final long serialVersionUID = 1884629235783787590L;

    public NOPMapTask(UUID uuid, Object source, Integer nreduce, IHashFunction hashFunction) {

        super(uuid, source, nreduce, hashFunction);

    }
    
    public void input(File file, InputStream is) throws Exception {

    }

}