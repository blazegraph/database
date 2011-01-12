package benchmark.model;

public class Person extends BSBMResource {
	private int nr;
	private String name;
	private String mbox_sha1sum;
	private String countryCode;
	
	public Person(int nr, String name, String countryCode, String mbox_sha1)
	{
		this.countryCode = countryCode;
		this.nr = nr;
		this.name = name;
		this.mbox_sha1sum = mbox_sha1;
	}

	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMbox_sha1sum() {
		return mbox_sha1sum;
	}

	public void setMbox_sha1sum(String mbox_sha1sum) {
		this.mbox_sha1sum = mbox_sha1sum;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	@Override
    public String toString()
	{
		return getURIref(nr, publisher);
	}
	
	public static String getURIref(int personNr, int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(RatingSite.getRatingSiteNS(ratingSiteNr));
		s.append("Reviewer");
		s.append(personNr);
		s.append(">");
		return s.toString();
	}
	
	public static String getPrefixed(int personNr, int ratingSiteNr) {
		StringBuffer s = new StringBuffer();
		s.append(RatingSite.getRatingSiteNSprefixed(ratingSiteNr));
		s.append("Reviewer");
		s.append(personNr);
		return s.toString();
	}
}
