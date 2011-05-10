package com.bigdata.rdf.sail.contrib;

import java.util.Properties;

import com.bigdata.rdf.sail.BigdataSail;

public class TestRollbacksTx extends TestRollbacks {

	public TestRollbacksTx() {
		super();
	}
	
	public TestRollbacksTx(String name) {
		super(name);
	}
	
	@Override
    public Properties getProperties() {
        
    	final Properties props = super.getProperties();

        // transactions are ON in this version of this class.
        props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
        
        return props;

	}
	
}
