package bd_ex2_gid.bd_ex2_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class SumAverageTuple implements WritableComparable<SumAverageTuple>{

	private DoubleWritable sum;
	private DoubleWritable average;
	
	public SumAverageTuple() {
		this.sum = new DoubleWritable(0);
		this.average = new DoubleWritable(0);
	}
	
	public SumAverageTuple(DoubleWritable average) {
		this.sum = new DoubleWritable(1);
		this.average = new DoubleWritable(average.get());
	}
	
	public SumAverageTuple(DoubleWritable sum, DoubleWritable average) {
		this.sum = new DoubleWritable(sum.get());
		this.average = new DoubleWritable(average.get());
	}

	public DoubleWritable getSum() {
		return this.sum;
	}

	public void setSum(double sum) {
		this.sum = new DoubleWritable(average.get());
	}

	public DoubleWritable getAverage() {
		return this.average;
	}

	public void setAverage(DoubleWritable average) {
		this.average = new DoubleWritable(average.get());
	}
	
	public void addNewValue(DoubleWritable newValue) {
		double sumOfValues = this.getAverage().get() * this.getSum().get() + newValue.get();
		double newSum = this.getSum().get() + 1.0;
		double newAvg = sumOfValues / newSum ;
		
		this.sum = new DoubleWritable(newSum);
		this.average = new DoubleWritable(newAvg);
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.sum.readFields(arg0);
		this.average.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.sum.write(arg0);
		this.average.write(arg0);
	}

	@Override
	public int compareTo(SumAverageTuple arg0) {
		return this.getAverage().compareTo(arg0.getAverage());
	}
	
	@Override
	public String toString() {
		return this.average.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((average == null) ? 0 : average.hashCode());
		result = prime * result + ((sum == null) ? 0 : sum.hashCode());
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
		SumAverageTuple other = (SumAverageTuple) obj;
		if (average == null) {
			if (other.average != null)
				return false;
		} else if (!average.equals(other.average))
			return false;
		if (sum == null) {
			if (other.sum != null)
				return false;
		} else if (!sum.equals(other.sum))
			return false;
		return true;
	}
	
}
