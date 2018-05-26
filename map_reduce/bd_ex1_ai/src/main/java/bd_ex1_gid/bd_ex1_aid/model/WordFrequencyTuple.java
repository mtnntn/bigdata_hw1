package bd_ex1_gid.bd_ex1_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordFrequencyTuple implements WritableComparable<WordFrequencyTuple>{

	private Text word = new Text("");
	private IntWritable frequency = new IntWritable(0);
	
	public WordFrequencyTuple() {
		this.word = new Text("");
		this.frequency = new IntWritable(0);
	}
	
	public WordFrequencyTuple(String word) {
		this.word.set(word);
		this.frequency = new IntWritable(0);
	}
	
	public WordFrequencyTuple(String word, int frequency) {
		this.word.set(word);
		this.frequency.set(frequency);
	}
	
	public WordFrequencyTuple(Text word, IntWritable frequency) {
		this.word.set(word);
		this.frequency.set(frequency.get());
	}
	
	public Text getWord() {
		return word;
	}

	public void setWord(Text word) {
		this.word.set(word);
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency.set(frequency.get());
	}
	
	public void increaseFrequency(int x) {
		this.frequency.set(this.frequency.get() + x);
	}
	
	public void increaseFrequency(IntWritable i) {
		this.increaseFrequency(i.get());
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.word.readFields(arg0);
		this.frequency.readFields(arg0);		
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		this.word.write(arg0);	
		this.frequency.write(arg0);	
	}
	
	@Override
	public int compareTo(WordFrequencyTuple arg0) {
		int res = -1 * this.getFrequency().compareTo(arg0.getFrequency());
		if (res == 0) {
			res = this.getWord().compareTo(arg0.getWord());
		}
		return res;	
	}
	
	@Override
	public String toString() {
		return this.getWord().toString().concat(" ").concat(this.getFrequency().toString());
	}
	
}
