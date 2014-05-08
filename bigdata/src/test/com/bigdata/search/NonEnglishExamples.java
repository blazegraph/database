package com.bigdata.search;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class NonEnglishExamples {
	private static final String BUNDLE_NAME = "com.bigdata.search.examples"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	private NonEnglishExamples() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
