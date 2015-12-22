package com.bigdata.bop.fed;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.journal.IBTreeManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;

/**
 * Factory for a {@link QueryEngine} or derived class.
 *
 * @author bryan
 * 
 * @see QueryEngineFactory#getInstance()
 * 
 * @see BLZG-1471 (Convert QueryEngineFactory to use getInstance() and public
 *      interface for accessor methods.)
 */
public interface IQueryEngineFactory {

	/**
	 * Singleton factory test (does not create the query controller) for
	 * standalone or scale-out.
	 * 
	 * @param indexManager
	 *            The database.
	 * 
	 * @return The query controller iff one has been obtained from the factory
	 *         and its weak reference has not been cleared.
	 */
	QueryEngine getExistingQueryController(IBTreeManager indexManager);

	/**
	 * Singleton factory for standalone or scale-out.
	 * 
	 * @param indexManager
	 *            The database.
	 * 
	 * @return The query controller.
	 */
	QueryEngine getQueryController(IIndexManager indexManager);

	/**
	 * Singleton factory for standalone.
	 * 
	 * @param indexManager
	 *            The index manager. Can be a {@link TemporaryStore} or
	 *            {@link Journal}.
	 * 
	 * @return The query controller.
	 */
	QueryEngine getStandaloneQueryController(IBTreeManager indexManager);

	/**
	 * New query controller for scale-out.
	 * <p>
	 * Note: This is NOT used for the {@link QueryEngine} that is embedded
	 * within the {@link DataService}. That instance is setup by the
	 * {@link DataService} itself and relies on a view of the shards as locally
	 * available to the {@link DataService}
	 * 
	 * @param fed
	 *            The federation.
	 * 
	 * @return The query controller.
	 */
	FederatedQueryEngine getFederatedQueryController(IBigdataFederation<?> fed);

	/**
	 * Return the #of live query controllers.
	 */
	int getQueryControllerCount();

}