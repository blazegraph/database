package com.bigdata.rdf.sail.config;

import java.util.Properties;

import org.openrdf.repository.Repository;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * A {@link RepositoryFactory} that creates {@link BigdataSailRepository}s 
 * based on RDF configuration data.
 */
public class BigdataRepositoryFactory implements RepositoryFactory {

	/**
	 * The type of repositories that are created by this factory.
	 */
	public static final String TYPE = "bigdata:BigdataRepository";

	public String getRepositoryType() {
		return TYPE;
	}

	public RepositoryImplConfig getConfig() {
		return new BigdataRepositoryConfig(TYPE);
	}

	public Repository getRepository(final RepositoryImplConfig config)
		throws RepositoryConfigException {
	
		if (!TYPE.equals(config.getType())) {
			throw new RepositoryConfigException(
                    "Invalid type: " + config.getType());
		}
		
		if (!(config instanceof BigdataRepositoryConfig)) {
			throw new RepositoryConfigException(
                    "Invalid type: " + config.getClass());
		}
		
        try {
            
			final BigdataRepositoryConfig bigdataConfig = (BigdataRepositoryConfig)config;
			final Properties properties = bigdataConfig.getProperties();
    		final BigdataSail sail = new BigdataSail(properties);
    		return new BigdataSailRepository(sail);
            
        } catch (Exception ex) {
            throw new RepositoryConfigException(ex);
        }
        
	}
}
