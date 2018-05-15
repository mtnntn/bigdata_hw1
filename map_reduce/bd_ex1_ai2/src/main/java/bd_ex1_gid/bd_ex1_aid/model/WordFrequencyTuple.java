package bd_ex1_gid.bd_ex1_aid.model;

public class WordFrequencyTuple implements Comparable<WordFrequencyTuple>{

	private String word = "";
	private int frequency = 0;

	public WordFrequencyTuple(String word, int frequency) {
		this.word = word;
		this.frequency = frequency;
	}
	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	
	@Override
	public int compareTo(WordFrequencyTuple arg0) {
		return -1 * Integer.valueOf(this.frequency).compareTo(Integer.valueOf(arg0.getFrequency()));
	}
	
	@Override
	public String toString() {
		return this.getWord()+"\t"+this.getFrequency();
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + frequency;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordFrequencyTuple other = (WordFrequencyTuple) obj;
		if (frequency != other.frequency)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

}
