
package com.bigdata.rdf.sail.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryImplConfigBase;

public class BigdataRepositoryConfig extends RepositoryImplConfigBase {

	/*-----------*
	 * Variables *
	 *-----------*/

	private String propertiesFile;

	/*--------------*
	 * Constructors *
	 *--------------*/

	public BigdataRepositoryConfig(final String type) {
		super(type);
	}
	
	/*---------*
	 * Methods *
	 *---------*/

	public String getPropertiesFile() {
		return propertiesFile;
	}

	public void setPropertiesFile(String propertiesFile) {
		this.propertiesFile = propertiesFile;
	}

    public Properties getProperties() 
            throws FileNotFoundException, IOException {
        
        if (propertiesFile == null) {
            return new Properties();
        }
        
        FileInputStream is = new FileInputStream(new File(propertiesFile));
        Properties props = new Properties();
        props.load(is);
        return props;
        
    }
    
	@Override
	public Resource export(Graph graph)
	{
		Resource implNode = super.export(graph);

		if (propertiesFile != null) {
			graph.add(implNode, BigdataConfigSchema.PROPERTIES, 
                    graph.getValueFactory().createLiteral(propertiesFile));
		}

		return implNode;
	}

	@Override
	public void parse(Graph graph, Resource implNode)
		throws RepositoryConfigException
	{
		super.parse(graph, implNode);

		try {
			Literal propertiesLit = GraphUtil.getOptionalObjectLiteral(
                    graph, implNode, BigdataConfigSchema.PROPERTIES);
			if (propertiesLit != null) {
				setPropertiesFile((propertiesLit).getLabel());
			} else {
                throw new RepositoryConfigException("Properties file required");
            }
		}
		catch (GraphUtilException e) {
			throw new RepositoryConfigException(e.getMessage(), e);
		}
	}
}
