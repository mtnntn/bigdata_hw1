package bd_ex1_gid.bd_ex1_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearWordTuple implements WritableComparable<YearWordTuple>{

	private Text year;
	private Text word;
	private IntWritable frequency;

	public YearWordTuple() {
		this.year = new Text();
		this.word = new Text();
		this.frequency = new IntWritable(0);
	}

	public YearWordTuple(Text year, Text word) {
		this.year = new Text(year);
		this.word = new Text(word);
		this.frequency = new IntWritable(0);
	}

	public YearWordTuple(Text year, Text word, IntWritable frequency) {
		this.year = new Text(year);
		this.word = new Text(word);
		this.frequency = new IntWritable(frequency.get());
	}

	public Text getYear() {
		return year;
	}

	public Text getWord() {
		return this.word;
	}

	public IntWritable getFrequency() {
		return this.frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}

	@Override
	public int compareTo(YearWordTuple arg0) {
		int result = this.getYear().compareTo(arg0.getYear());
		if(result == 0) {
			result = -1 * this.getFrequency().compareTo(arg0.getFrequency());
		}
		if(result == 0) {
			result = this.getWord().compareTo(arg0.getWord());
		}
		return result;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		YearWordTuple other = (YearWordTuple) obj;
		if (frequency == null) {
			if (other.frequency != null)
				return false;
		} else if (!frequency.equals(other.frequency))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.year.readFields(arg0);
		this.word.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.year.write(arg0);
		this.word.write(arg0);
	}

	@Override
	public String toString() {
		return this.getYear().toString() + " " + this.getWord().toString();
	}

}
