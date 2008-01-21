package com.bigdata.repo;

import java.util.Iterator;

/**
 * Interface for a rest-ful content repository.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ContentRepository 
{

    /**
     * Fetch a single document object based on a URI.
     * 
     * @param id
     *            the identifier of the document to fetch
     * 
     * @return the document -or- null if there is no such document.
     */
    Document read( String id);
    
    /**
     * Create a new persistent document in this repository based on the metadata
     * and content in the supplied document object.
     * 
     * @param document
     *            an object containing the content and metadata to persist
     * 
     * @throws ExistsException
     *             if there is already a document with the same identifier.
     */
    void create( Document document );
    
    /**
     * Update an existing persistent document in this repository based on the
     * metadata and content in the supplied document object. The document to be
     * updated will be identified by the {@link DocumentHeader#getId()} method.
     * 
     * @param document
     *            an object containing the content and metadata to update
     */
    void update( Document document );
    
    /**
     * Delete a single document.
     * 
     * @param id
     *            the identifier of the document to delete
     * 
     * @return true iff the identified document was deleted.
     */
    boolean delete(String id);
    
    /**
     * Delete all documents in the identified key range.
     * <p>
     * Note: If you assign identifiers using a namespace then you can use this
     * method to rapidly delete all documents within that namespace.
     * 
     * @param fromId
     *            The identifier of the first document to be deleted or
     *            <code>null</code> if there is no lower bound.
     * @param toId
     *            The identifier of the first document that will NOT be deleted
     *            or <code>null</code> if there is no upper bound.
     */
    void deleteAll(String fromId, String toId);
    
    /**
     * Return a listing of the documents and metadata about them in this
     * repository.
     * <p>
     * Note: If you assign identifiers using a namespace then you can use this
     * method to efficiently visit all documents within that namespace.
     * 
     * @param fromId
     *            The identifier of the first document to be visited or
     *            <code>null</code> if there is no lower bound.
     * @param toId
     *            The identifier of the first document that will NOT be visited
     *            or <code>null</code> if there is no upper bound.
     * 
     * @return an iterator of {@link DocumentHeader}s.
     */
    Iterator<? extends DocumentHeader> getDocumentHeaders(String fromId,
            String toId);

    /**
     * Full text search against the indexed documents.
     * 
     * @param query
     *            A query.
     * 
     * @return An iterator visiting the identifiers of the documents in order of
     *         decreasing relevance to the query.
     * 
     * @todo return more metadata about the search results.
     * 
     * @todo allow fromId, toId to restrict to a given namespace?
     * 
     * @todo register analyzers against MIME types and index those that are
     *       selected on index or update.
     */
    Iterator<String> search(String query);
    
    /**
     * Close the client's connection to the repository.
     * 
     * @throws Exception
     */
    void close();

    /**
     * Thrown when the identified document already exists.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ExistsException extends RuntimeException {
        
        public ExistsException(String id) {

            super("id="+id);
            
        }
        
    }
    
    /**
     * Thrown when the identified document was not found.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class NotFoundException extends RuntimeException {
        
        public NotFoundException(String id) {

            super("id="+id);
            
        }
        
    }
    
}
