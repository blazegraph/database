package com.bigdata.bfs;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import com.bigdata.sparse.IRowStoreConstants;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;

/**
 * A read-only view of a {@link Document} that has been read from a
 * {@link BigdataFileSystem}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RepositoryDocumentImpl implements DocumentHeader, Document 
{
    
    final private BigdataFileSystem repo;
    
    final private String id;
    
    /**
     * The result of the atomic read on the file's metadata. This
     * representation is significantly richer than the current set of
     * property values.
     */
    final ITPS tps;

    /**
     * The current version identifer -or- <code>-1</code> iff there is no
     * current version for the file (including when there is no record of
     * any version for the file).
     */
    final int version;
    
    /**
     * The property set for the current file version.
     */
    final private Map<String,Object> metadata;

    /**
     * Read the metadata for the current version of the file from the
     * repository.
     * 
     * @param id
     *            The file identifier.
     * @param tps
     *            The logical row describing the metadata for some file in
     *            the repository.
     */
    public RepositoryDocumentImpl(final BigdataFileSystem repo, final String id,
            final ITPS tps) {
        
        if (repo == null)
            throw new IllegalArgumentException();

        if (id == null)
            throw new IllegalArgumentException();
        
        this.repo = repo;
        
        this.id = id;
        
        this.tps = tps;
        
        if (tps != null) {

            ITPV tmp = tps.get(FileMetadataSchema.VERSION);
            
            if (tmp.getValue() != null) {

                /*
                 * Note the current version identifer.
                 */
                
                this.version = (Integer) tmp.getValue();

                /*
                 * Save a simplifed view of the propery set for the current
                 * version.
                 */
                
                this.metadata = tps.asMap();

                BigdataFileSystem.log.info("id="+id+", current version="+version);

            } else {
                
                /*
                 * No current version.
                 */
                
                this.version = -1;

                this.metadata = null;
                
                BigdataFileSystem.log.warn("id="+id+" : no current version");

            }

        } else {
            
            /*
             * Nothing on record for that file identifier.
             */
            
            this.version = -1;
            
            this.metadata = null;
            
            BigdataFileSystem.log.warn("id="+id+" : no record of any version(s)");

        }
        
        if (BigdataFileSystem.DEBUG && metadata != null) {

            Iterator<Map.Entry<String,Object>> itr = metadata.entrySet().iterator();
            
            while(itr.hasNext()) {
                
                Map.Entry<String, Object> entry = itr.next();
                
                BigdataFileSystem.log.debug("id=" + id + ", version=" + getVersion() + ", ["
                        + entry.getKey() + "]=[" + entry.getValue() + "]");
                
            }

        }

    }
    
    /**
     * Read the metadata for the current version of the file from the
     * repository.
     * 
     * @param id
     *            The file identifier.
     */
    public RepositoryDocumentImpl(final BigdataFileSystem repo, final String id) {

        this(repo, id, repo.getFilleMetadataIndex().read(
                BigdataFileSystem.metadataSchema, id,
                IRowStoreConstants.MIN_TIMESTAMP,
                IRowStoreConstants.CURRENT_ROW, null/* filter */));

    }

    /**
     * Assert that a version of the file existed when this view was constructed.
     * 
     * @throws IllegalStateException
     *             unless a version of the file existed at the time that this
     *             view was constructed.
     */
    final protected void assertExists() {

        if (version == -1) {

            throw new IllegalStateException("No current version: id="+id);
            
        }
        
    }
    
    final public boolean exists() {
        
        return version != -1;
        
    }
    
    final public int getVersion() {

        assertExists();

        return (Integer)metadata.get(FileMetadataSchema.VERSION);

    }

    /**
     * Note: This is obtained from the earliest available timestamp of the
     * {@link FileMetadataSchema#ID} property.
     */
    final public long getEarliestVersionCreateTime() {
        
        assertExists();
        
        Iterator<ITPV> itr = tps.iterator();
        
        while(itr.hasNext()) {
            
            ITPV tpv = itr.next();
            
            if(tpv.getName().equals(FileMetadataSchema.ID)) {
                
                return tpv.getTimestamp();
                
            }
            
        }
        
        throw new AssertionError();
        
    }

    final public long getVersionCreateTime() {

        assertExists();
        
        /*
         * The timestamp for the most recent value of the VERSION property.
         */
        
        final long createTime = tps.get(FileMetadataSchema.VERSION)
                .getTimestamp();
        
        return createTime;
        
    }

    final public long getMetadataUpdateTime() {
        
        assertExists();
        
        /*
         * The timestamp for the most recent value of the ID property.
         */
        
        final long metadataUpdateTime = tps.get(FileMetadataSchema.ID)
                .getTimestamp();
        
        return metadataUpdateTime;

    }

    /**
     * Return an array containing all non-eradicated values of the
     * {@link FileMetadataSchema#VERSION} property for this file as of the time
     * that this view was constructed.
     * 
     * @see BigdataFileSystem#getAllVersionInfo(String)
     */
    final public ITPV[] getAllVersionInfo() {
        
        return repo.getAllVersionInfo(id);
        
    }
    
    final public InputStream getInputStream() {

        assertExists();
        
        return repo.inputStream(id,getVersion());
        
    }
    
    final public Reader getReader() throws UnsupportedEncodingException {

        assertExists();

        return repo.reader(id, getVersion(), getContentEncoding());

    }

    final public String getContentEncoding() {

        assertExists();
        
        return (String)metadata.get(FileMetadataSchema.CONTENT_ENCODING);
        
    }

    final public String getContentType() {
     
        assertExists();

        return (String)metadata.get(FileMetadataSchema.CONTENT_TYPE);
        
    }

    final public String getId() {

        return id;
        
    }
    
    final public Object getProperty(String name) {
        
        return metadata.get(name);
        
    }
    
    final public Map<String,Object> asMap() {
        
        assertExists();

        return Collections.unmodifiableMap( metadata );
        
    }

}