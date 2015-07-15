package com.bigdata.bfs;

import java.util.Map;

/**
 * Document header interface for {@link Document}s stored in the
 * {@link IContentRepository}.
 * 
 * @author mike@systap.com
 * @version $Id$
 */
public interface DocumentHeader
{

    /**
     * The unique identifier for the document within the
     * {@link IContentRepository}.
     */
    String getId();
    
    /**
     * The MIME type for the document.
     */
    String getContentType();

    /**
     * The character set encoding that MUST be used to interpret the byte[]
     * returned by {@link Document#getContent()}.
     */
    String getContentEncoding();

    /**
     * Get an arbitrary property value.
     * 
     * @param property
     *            The property name.
     *            
     * @return The value.
     */
    Object getProperty(String name);
    
    /**
     * Returns a read-only view of the document metadata.
     */
    Map<String,Object> asMap();
    
    /**
     * Return <code>true</code> iff a version of the file existed at the
     * time that this view was constructed.
     */
    boolean exists();
    
    /**
     * Return the version identifier.
     * 
     * @throws IllegalStateException
     *             unless a version of the file existed at the time that
     *             this view was constructed.
     */
    int getVersion();

    /**
     * The time at which the earliest version of this file still on record
     * was created (historical file metadata can be eradicated through
     * compacting merges).
     */
    long getEarliestVersionCreateTime();
    
    /**
     * The time at which the current version of this file was created.
     * 
     * @throws IllegalStateException
     *             unless a version of the file existed at the time that
     *             this view was constructed.
     */
    long getVersionCreateTime();
    
    /**
     * The time at which the metadata for the current version of this file
     * was last updated.
     * 
     * @throws IllegalStateException
     *             unless a version of the file existed at the time that
     *             this view was constructed.
     */
    long getMetadataUpdateTime();    

}
