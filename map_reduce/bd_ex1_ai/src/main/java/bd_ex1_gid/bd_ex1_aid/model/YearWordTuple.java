package bd_ex1_gid.bd_ex1_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearWordTuple implements WritableComparable<YearWordTuple>{

	private Text year = new Text("");
	private Text word = new Text("");

	public YearWordTuple() {
		this.year = new Text("");
		this.word = new Text("");
	}
	
	public YearWordTuple(Text year, Text word) {
		this.year.set(year);
		this.word.set(word);
	}

	public YearWordTuple(String year, String word) {
		this.year.set(year);
		this.word.set(word);
	}

	public Text getYear() {
		return year;
	}
	
	public void setYear(Text year) {
		this.getYear().set(year);
	}

	public Text getWord() {
		return this.word;
	}
	
	public void setWord(Text word) {
		this.getWord().set(word);
	}

	@Override
	public int compareTo(YearWordTuple arg0) {
		int result = this.getYear().compareTo(arg0.getYear());
		if(result == 0) {
			result = this.getWord().compareTo(arg0.getWord());
		}
		return result;
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
		return this.getYear().toString().concat(" ").concat(this.getWord().toString());
	}

}
