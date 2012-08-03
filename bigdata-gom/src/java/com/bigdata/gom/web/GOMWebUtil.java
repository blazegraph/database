package com.bigdata.gom.web;

import javax.servlet.http.HttpServletRequest;

import com.bigdata.gom.om.ObjectManager;

/**
 * A simple utility class that can be initialized with a ServletRequest and used to simplify dynamic
 * web pages.
 * 
 * @author Martyn Cutcher
 */

public class GOMWebUtil {
	final ObjectManager m_om;
	
	public static ObjectManager getObjectManager(HttpServletRequest request) {
		return (ObjectManager) request.getSession().getServletContext().getAttribute(ObjectManager.class.getName());
	}
	
	public GOMWebUtil(HttpServletRequest request) {
		m_om = getObjectManager(request);
	}
	
	public ObjectManager getObjectManager() {
		return m_om;
	}
}
