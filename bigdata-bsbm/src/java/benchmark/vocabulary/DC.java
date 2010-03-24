package benchmark.vocabulary;

import java.util.HashMap;

public class DC {
	//The Namespace of this vocabulary as String
	public static final String NS = "http://purl.org/dc/elements/1.1/";
	
	public static final String PREFIX = "dc:";
	
	private static HashMap<String, String> uriMap = new HashMap<String, String>();
	
	/*
	 * For prefixed versions
	 */
	public static String prefixed(String string) {
		if(uriMap.containsKey(string)) {
			return uriMap.get(string);
		}
		else {
			String newValue = PREFIX + string;
			uriMap.put(string, newValue);
			return newValue;
		}
	}

	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
	
	//Properties
	public static final String publisher = NS + "publisher";
	
	public static final String date = NS + "date";
	
	public static final String title = NS + "title";
}
