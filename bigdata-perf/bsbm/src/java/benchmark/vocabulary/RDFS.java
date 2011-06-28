package benchmark.vocabulary;

import java.util.HashMap;

public class RDFS {
	//The namespace of this vocabulary as String
    public static final String NS="http://www.w3.org/2000/01/rdf-schema#";

    //Get the URI of this vocabulary
	public static String getURI() { return NS; }

	public static final String PREFIX = "rdfs:";
	
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
	
	//Resources
    public static final String Datatype =  NS+"Datatype";
    public static final String Literal =  NS+"Literal";
    public static final String Resource =  NS+"Resource";

    //Properties
    public static final String comment =  NS+"comment";
    public static final String label =  NS+"label";
    public static final String subClassOf  = NS+"subClassOf";
}
