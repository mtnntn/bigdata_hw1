package bd_ex2_gid.bd_ex2_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductYearTuple implements WritableComparable<ProductYearTuple>{

	private Text year;
	private Text productId;

	public ProductYearTuple() {
		this.year = new Text();
		this.productId = new Text();
	}

	public ProductYearTuple(Text year, Text productId) {
		this.year = new Text(year);
		this.productId = new Text(productId);
	}

	public ProductYearTuple(Text year, Text productId, DoubleWritable frequency) {
		this.year = new Text(year);
		this.productId = new Text(productId);
	}

	public Text getYear() {
		return year;
	}

	public Text getProductId() {
		return this.productId;
	}

	@Override
	public int compareTo(ProductYearTuple arg0) {
		int result = this.getProductId().compareTo(arg0.getProductId());
		if(result == 0) {
			result = this.getYear().compareTo(arg0.getYear());
		}
		return result;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.productId.readFields(arg0);
		this.year.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.productId.write(arg0);
		this.year.write(arg0);
	}
	
	@Override
	public String toString() {
		return this.getProductId().toString() + " " + this.getYear().toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((productId == null) ? 0 : productId.hashCode());
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
		ProductYearTuple other = (ProductYearTuple) obj;
		if (productId == null) {
			if (other.productId != null)
				return false;
		} else if (!productId.equals(other.productId))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}
	
}
