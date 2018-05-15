package bd_ex3_gid.bd_ex3_aid.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase extends Reducer<Text, Text, Text, IntWritable> {

	// prodId -> list(userId)
	private Map<String, List<String>> productUsers = new TreeMap<String, List<String>>();	
	// userId -> list(prodId)
	private Map<String, List<String>> userProducts = new TreeMap<String, List<String>>();
	// prodId -> list(userId)
	private Map<String, Set<String>> itemCouples = new TreeMap<String, Set<String>>();
	
	@Override
	public void reduce(Text productId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text t : values) {
			this.addUserToProductUsers(productId.toString(), t.toString());
			this.addProductToUserProducts(t.toString(), productId.toString());
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {			
		for(String product : productUsers.keySet()) {
			for(String user : productUsers.get(product)) {
				for(String userProduct : this.userProducts.get(user)) {
					if(!userProduct.equals(product)) {
						String pkey1 = product + "-" + userProduct;
						String pkey2 = userProduct + "-" + product;
						String pkey = (product.compareTo(userProduct) <= 0)? pkey1 : pkey2 ;
						if(! itemCouples.containsKey(pkey)) {
							itemCouples.put(pkey, new TreeSet<String>());
						}
						itemCouples.get(pkey).add(user);
					}
				}
			}
		}
		for(String i : itemCouples.keySet()) {
			String[] splitted = i.split("-");
			context.write(new Text(splitted[0] + "\t" + splitted[1]), new IntWritable(itemCouples.get(i).size()));
		}
	}

	private void addProductToUserProducts(String userID, String productID) {
		addToMap(userID, productID, this.userProducts);
	}
	
	private void addUserToProductUsers(String productID, String userID) {
		addToMap(productID, userID, this.productUsers);
	}
	
	private void addToMap(String k, String v, Map<String, List<String>> m) {
		List<String> list = m.get(k);
		if(list == null)
			list = new ArrayList<String>();
		if(list.indexOf(v)==-1) {
			list.add(v);
			m.put(k, list);
		}
	}
	
}
