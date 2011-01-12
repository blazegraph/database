package benchmark.model;

public class Review extends BSBMResource {
	private int nr;
	private Integer product;
	private Integer producerOfProduct;
	private int person;
	private long reviewDate;
	private String title;
	private String text;
	private Integer[] ratings;
	private int language;//Language-Byte-Code
	
	public Review(int nr,Integer forProductNr, int byPersonNr, long reviewDate,
					String title, String text, Integer[] ratings, int languageCode,
					Integer producerOfProduct) {
		this.nr = nr;
		product = forProductNr;
		person  = byPersonNr;
		this.reviewDate = reviewDate;
		this.title = title;
		this.text = text;
		this.ratings = ratings;
		this.language = languageCode;
		this.producerOfProduct = producerOfProduct;
	}

	public int getNr() {
		return nr;
	}

	public Integer getProduct() {
		return product;
	}

	public int getPerson() {
		return person;
	}

	public long getReviewDate() {
		return reviewDate;
	}

	public String getTitle() {
		return title;
	}

	public String getText() {
		return text;
	}

	public Integer[] getRatings() {
		return ratings;
	}
	
	@Override
    public String toString()
	{
		return getURIref(nr, publisher);
	}
	
	public static String getURIref(int reviewNr, int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(RatingSite.getRatingSiteNS(ratingSiteNr));
		s.append("Review");
		s.append(reviewNr);
		s.append(">");
		return s.toString();
	}

	public static String getPrefixed(int reviewNr, int ratingSiteNr)
	{
		StringBuffer s = new StringBuffer();
		s.append(RatingSite.getRatingSiteNSprefixed(ratingSiteNr));
		s.append("Review");
		s.append(reviewNr);
		return s.toString();
	}

	public int getLanguage() {
		return language;
	}

	public Integer getProducerOfProduct() {
		return producerOfProduct;
	}

	public void setProducerOfProduct(Integer producerOfProduct) {
		this.producerOfProduct = producerOfProduct;
	}
}
