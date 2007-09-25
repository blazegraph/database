package com.bigdata.service.mapReduce;

import java.util.Iterator;

/**
 * Interface responsible for identifying input sources for map tasks. For
 * example, by scanning the files in a local directory on the map server.
 */
public interface IMapSource {
    
    /**
     * An iterator that will visit each source to be read by the map task in
     * turn.
     */
    public Iterator<Object> getSources();

}