/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

 */
package com.bigdata.rdf.sail;

import java.io.IOException;

import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailException;

/**
 * Blazegraph specific {@link SailRepository} implementation class.
 * 
 * @see BigdataSailRepositoryConnection
 */
public class BigdataSailRepository extends SailRepository {
   
    public BigdataSailRepository(final BigdataSail sail) {
    
        super(sail);
        
    }
    
//    /* Gone since BLZG-2041: This was accessing the AbstractTripleStore without a Connection.
//    
//     * @see BLZG-2041 BigdataSail should not locate the AbstractTripleStore
//     * until a connection is requested
//     */
//    @Deprecated // This is accessing the AbstractTripleStore without a Connection. 
//    public AbstractTripleStore getDatabase() {
//        
//        return ((BigdataSail) getSail()).getDatabase();
//        
//    }

    @Override
    public BigdataSail getSail() {

        return (BigdataSail) super.getSail();

    }

    /**
     * {@inheritDoc}
     * <p>
     * The correct pattern for obtaining an updatable connection, doing work
     * with that connection, and committing or rolling back that update is as
     * follows.
     * 
     * <pre>
     * 
     * BigdataSailConnection conn = null;
     * boolean ok = false;
     * try {
     *     conn = repo.getConnection();
     *     doWork(conn);
     *     conn.commit();
     *     ok = true;
     * } finally {
     *     if (conn != null) {
     *         if (!ok) {
     *             conn.rollback();
     *         }
     *         conn.close();
     *     }
     * }
     * </pre>
     * 
     * @see BigdataSail#getConnection()
     * @see #getUnisolatedConnection()
     */
    @Override
    public BigdataSailRepositoryConnection getConnection() 
            throws RepositoryException {
        
        try {
        
            return new BigdataSailRepositoryConnection(this, 
                getSail().getConnection());
            
        } catch (SailException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }

	/**
	 * Obtain a read-only connection to the database at the last commit point. A
	 * read-only connection should be used for all pure-readers, as the
	 * connection will not be blocked by concurrent writers.
	 * 
	 * @return a read-only connection to the database
	 * 
	 * @see BigdataSail#getReadOnlyConnection()
	 */
    public BigdataSailRepositoryConnection getReadOnlyConnection() 
        throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getSail().getReadOnlyConnection());
    }

	/**
	 * Obtain a read-only connection to the database from a historical commit
	 * point. A read-only connection should be used for all pure-readers, as the
	 * connection will not be blocked by concurrent writers.
	 * 
	 * @return a read-only connection to the database
	 * 
	 * @see BigdataSail#getReadOnlyConnection(long)
	 */
    public BigdataSailRepositoryConnection getReadOnlyConnection(
         final long timestamp) throws RepositoryException {
        
        return new BigdataSailRepositoryConnection(this, 
            getSail().getReadOnlyConnection(timestamp));
        
    }

	/**
	 * Return a connection backed by a read-write transaction.
	 * @throws InterruptedException 
	 * 
	 * @see BigdataSail#getReadWriteConnection()
	 */
    public BigdataSailRepositoryConnection getReadWriteConnection() 
        throws RepositoryException, InterruptedException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getSail().getReadWriteConnection());
            
        } catch (IOException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }
    
    /**
     * Return an unisolated connection to the database. Only one of these
     * allowed at a time.
     * <p>
     * The correct pattern for obtaining an updatable connection, doing work
     * with that connection, and committing or rolling back that update is as
     * follows.
     * 
     * <pre>
     * 
     * BigdataSailConnection conn = null;
     * boolean ok = false;
     * try {
     *     conn = repo.getConnection();
     *     doWork(conn);
     *     conn.commit();
     *     ok = true;
     * } finally {
     *     if (conn != null) {
     *         if (!ok) {
     *             conn.rollback();
     *         }
     *         conn.close();
     *     }
     * }
     * </pre>
     * 
     * @return unisolated connection to the database
     * 
     * @see BigdataSail#getUnisolatedConnection()
     * @see #getConnection()
     */
    public BigdataSailRepositoryConnection getUnisolatedConnection() 
        throws RepositoryException {
        
        try {
            
            return new BigdataSailRepositoryConnection(this, 
                getSail().getUnisolatedConnection());
            
        } catch (InterruptedException e) {
            
            throw new RepositoryException(e);
            
        }
        
    }

}
