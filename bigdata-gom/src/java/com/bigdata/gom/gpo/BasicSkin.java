package com.bigdata.gom.gpo;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

/**
 * The BasicSkin provides standard type wrappers to help with conversions
 * 
 * @author Martyn Cutcher
 */
public class BasicSkin implements IGenericSkin {
	
	final protected IGPO m_gpo;
	final protected ValueFactory m_vf;
	
	public BasicSkin(final IGPO gpo) {
		m_gpo = gpo;
		m_vf = gpo.getObjectManager().getValueFactory();
	}
	
	public void rollback() {
		((GPO) m_gpo).setMaterialized(false); // forces reload
	}
	
	public void setValue(final URI property, final Value value) {
    	m_gpo.setValue(property, value);
    }

    public void setValue(final URI property, final String value) {
    	m_gpo.setValue(property, m_vf.createLiteral(value));
    }

    public void setValue(final URI property, final int value) {
    	m_gpo.setValue(property, m_vf.createLiteral(value));
    }

    public void setValue(final URI property, final double value) {
    	m_gpo.setValue(property, m_vf.createLiteral(value));
    }

    public void setValue(final URI property, final XMLGregorianCalendar value) {
    	m_gpo.setValue(property, m_vf.createLiteral(value));
    }
    
	public void setValue(final String property, final Value value) {
    	m_gpo.setValue(m_vf.createURI(property), value);
    }

    public void setValue(final String property, final String value) {
    	m_gpo.setValue(m_vf.createURI(property), m_vf.createLiteral(value));
    }

    public void setValue(final String property, final int value) {
    	m_gpo.setValue(m_vf.createURI(property), m_vf.createLiteral(value));
    }

    public void setValue(final String property, final double value) {
    	m_gpo.setValue(m_vf.createURI(property), m_vf.createLiteral(value));
    }

    public void setValue(final String property, final XMLGregorianCalendar value) {
    	m_gpo.setValue(m_vf.createURI(property), m_vf.createLiteral(value));
    }

	@Override
	public IGPO asGeneric() {
		return m_gpo;
	}

	public int getIntValue(final URI key) {
		final Value v = m_gpo.getValue(key);
		
		if (v instanceof Literal) {
			return ((Literal) v).intValue();
		} else {	
			return 0;
		}
	}

	public double getDoubleValue(final URI key) {
		final Value v = m_gpo.getValue(key);
		
		if (v instanceof Literal) {
			return ((Literal) v).doubleValue();
		} else {	
			return 0;
		}
	}

	public boolean getBooleanValue(final URI key) {
		final Value v = m_gpo.getValue(key);
		
		if (v instanceof Literal) {
			return ((Literal) v).booleanValue();
		} else {	
			return false;
		}
	}

	public String getStringValue(final URI key) {
		final Value v = m_gpo.getValue(key);
		
		if (v instanceof Literal) {
			return ((Literal) v).stringValue();
		} else {	
			return null;
		}
	}

	public IGPO getGPOValue(final URI key) {
		final Value v = m_gpo.getValue(key);
		
		if (v instanceof Resource) {
			return m_gpo.getObjectManager().getGPO((Resource) v);
		} else {	
			return null;
		}
	}

	public int getInt(final String key) {
		return getIntValue(m_vf.createURI(key));
	}

	public double getDouble(final String key) {
		return getDoubleValue(m_vf.createURI(key));
	}

	public boolean getBoolean(final String key) {
		return getBooleanValue(m_vf.createURI(key));
	}

	public String getString(final String key) {
		return getStringValue(m_vf.createURI(key));
	}

}
