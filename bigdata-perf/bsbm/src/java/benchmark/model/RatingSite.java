package benchmark.model;

import benchmark.vocabulary.*;

public class RatingSite {
	public static String getRatingSiteNS(int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append(BSBM.INST_NS);
		s.append("dataFromRatingSite");
		s.append(ratingSiteNr);
		s.append("/");
		return s.toString();
	}
	
	public static String getURIref(int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(getRatingSiteNS(ratingSiteNr));
		s.append("RatingSite");
		s.append(ratingSiteNr);
		s.append(">");
		return s.toString();
	}
	
	public static String getRatingSiteNSprefixed(int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("dataFromRatingSite");
		s.append(ratingSiteNr);
		s.append(":");
		return s.toString();
	}
	
	public static String getPrefixed(int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append(getRatingSiteNSprefixed(ratingSiteNr));
		s.append("RatingSite");
		s.append(ratingSiteNr);
		return s.toString();
	}
}
