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
public class BigdataStoreFactory implements SailFactory {

	/**
	 * The type of repositories that are created by this factory.
	 * 
	 * @see SailFactory#getSailType()
	 */
	public static final String SAIL_TYPE = "bigdata:BigdataSail";

	/**
	 * Returns the Sail's type: <tt>bigdata:BigdataSail</tt>.
	 */
	public String getSailType() {
		return SAIL_TYPE;
	}

	public SailImplConfig getConfig() {
		return new BigdataStoreConfig();
	}

	public Sail getSail(SailImplConfig config)
		throws SailConfigException
	{
		if (!SAIL_TYPE.equals(config.getType())) {
			throw new SailConfigException(
                    "Invalid Sail type: " + config.getType());
		}

        try {
            
            Properties properties = null;
    		if (config instanceof BigdataStoreConfig) {
    			BigdataStoreConfig bigdataConfig = (BigdataStoreConfig)config;
                properties = bigdataConfig.getProperties();
    		} else {
    		    properties = new Properties();
            }
    
    		return new BigdataSail(properties);
            
        } catch (Exception ex) {
            throw new SailConfigException(ex);
        }
        
	}
}
