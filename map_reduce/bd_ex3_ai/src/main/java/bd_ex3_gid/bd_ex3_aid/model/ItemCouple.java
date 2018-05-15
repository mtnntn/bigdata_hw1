package bd_ex3_gid.bd_ex3_aid.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ItemCouple implements WritableComparable<ItemCouple>{

	private Text key = new Text("");
	private static final Text SEP = new Text("/");
	private Set<Text> users = new HashSet<Text>();

	public ItemCouple(String i1, String i2) {
		if(i1.compareTo(i2)<=0)
			key.set(i1.concat(SEP.toString()).concat(i2));
		else
			key.set(i2.concat(SEP.toString()).concat(i1));
	}

	public Text getKey() {
		return this.key;
	}

	public void setKey(Text key) {
		this.key.set(key);
	}

	public Set<Text> getUsers() {
		return users;
	}

	public void setUsers(Set<Text> users) {
		this.users = new HashSet<Text>();
		this.users.addAll(users);
	}
	
	public int getNumbersOfCommonUsers() {
		return (this.users==null)? -1 : this.users.size();
	}
	
	public ItemCouple mergeItemCouples(ItemCouple arg0) {
		this.getUsers().addAll(arg0.getUsers());
		return this;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.key.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		String[] split = key.toString().split(SEP.toString());
		Text t1 = new Text(split[0]);
		Text t2 = new Text(split[1]);
		t1.write(arg0);
		t2.write(arg0);
	}

	@Override
	public int compareTo(ItemCouple arg0) {
		int res = this.key.compareTo(arg0.getKey());
		if(res == 0) {
			String[] split = key.toString().split(SEP.toString());
			res = split[0].compareTo(split[1]);
		}
		if(res == 0)
			res = (this.getNumbersOfCommonUsers() < arg0.getNumbersOfCommonUsers())? -1 : 1 ;
		return res;
	}
	
	@Override
	public String toString() {
		String[] split = key.toString().split(SEP.toString());
		return split[0] + " " + split[1];
	}

	@Override
	public int hashCode() {
		return 31 * ((key == null) ? 0 : key.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemCouple other = (ItemCouple) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

}
