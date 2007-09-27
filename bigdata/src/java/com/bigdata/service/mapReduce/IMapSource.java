package com.bigdata.service.mapReduce;

import java.util.Iterator;

/**
 * Interface responsible for identifying input sources for map tasks. For
 * example, by scanning the files in a local directory on the map server.
 * 
 * @todo this needs to be generalized to handle more kinds of inputs, including
 *       reading from an index, a scale-out index (whether hash or range
 *       partitioned), or a split that can be divided into "splits".
 */
public interface IMapSource {
    
    /**
     * An iterator that will visit each source to be read by the map task in
     * turn.
     */
    public Iterator<Object> getSources();

}