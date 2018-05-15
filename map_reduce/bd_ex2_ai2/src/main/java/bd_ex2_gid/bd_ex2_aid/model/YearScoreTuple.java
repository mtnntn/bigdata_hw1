package bd_ex2_gid.bd_ex2_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearScoreTuple implements WritableComparable<YearScoreTuple>{

		private Text year;
		private DoubleWritable score;

		public YearScoreTuple() {
			this.year = new Text();
			this.score = new DoubleWritable(0.0);
		}

		public YearScoreTuple(Text year, DoubleWritable score) {
			this.year = new Text(year);
			this.score = new DoubleWritable(score.get());
		}

		public Text getYear() {
			return year;
		}

		public void setYear(Text year) {
			this.year = year;
		}

		public DoubleWritable getScore() {
			return score;
		}

		public void setScore(DoubleWritable score) {
			this.score = score;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			this.year.readFields(arg0);
			this.score.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			this.year.write(arg0);
			this.score.write(arg0);
		}

		@Override
		public int compareTo(YearScoreTuple arg0) {
			int result = this.getYear().compareTo(arg0.getYear());
			return result;
		}
		
		@Override
		public String toString() {
			return this.year.toString() + " " + this.score.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((score == null) ? 0 : score.hashCode());
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
			YearScoreTuple other = (YearScoreTuple) obj;
			if (score == null) {
				if (other.score != null)
					return false;
			} else if (!score.equals(other.score))
				return false;
			if (year == null) {
				if (other.year != null)
					return false;
			} else if (!year.equals(other.year))
				return false;
			return true;
		}
	
}
