package com.bigdata.rdf.sail.config;

import java.util.Properties;

import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

import com.bigdata.rdf.sail.BigdataSail;

/**
 * A {@link SailFactory} that creates {@link BigdataSail}s based on RDF
 * configuration data.
 */
public class BigdataSailFactory implements SailFactory {

	/**
	 * The type of sails that are created by this factory.
	 */
	public static final String TYPE = "bigdata:BigdataSail";

	public String getSailType() {
		return TYPE;
	}

	public SailImplConfig getConfig() {
		return new BigdataSailConfig(TYPE);
	}

	public Sail getSail(final SailImplConfig config)
		throws SailConfigException {
	
		if (!TYPE.equals(config.getType())) {
			throw new SailConfigException(
                    "Invalid type: " + config.getType());
		}

		if (!(config instanceof BigdataSailConfig)) {
			throw new SailConfigException(
                    "Invalid type: " + config.getClass());
		}
		
        try {
            
			final BigdataSailConfig bigdataConfig = (BigdataSailConfig)config;
			final Properties properties = bigdataConfig.getProperties();
    		return new BigdataSail(properties);
            
        } catch (Exception ex) {
            throw new SailConfigException(ex);
        }
        
	}
}
