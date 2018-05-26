package bd_ex3_gid.bd_ex3_aid.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerPhase extends Reducer<Text, Text, Text, IntWritable> {

	private static final String SEPARATOR = "-";
	private static final String OUT_SEPARATOR ="\t";
	
	// product1-product2 -> # of common users 
	private Map<String, Integer> itemCouples = new TreeMap<String, Integer>();	
	
	@Override
	public void reduce(Text userId, Iterable<Text> products, Context context) throws IOException, InterruptedException {
		
		Set<String> alreadyAdded = new HashSet<String>(); // Allow to filter duplicates for the current user.
		
		List<String> prods = new ArrayList<String>();	// List of the products for the user.
		for(Text s : products)
			prods.add(s.toString());
						
		for(String product1 : prods) {	// Match each product in the list of products, with the others products of the list.
			for(String product2: prods) {
				if(! product1.equals(product2) ) {	
					String key = this.getKey(product1, product2);
					if(!alreadyAdded.contains(key)) {
						Integer val = itemCouples.get(key);
						if(val == null)
							val = 0;
						val += 1;
						itemCouples.put(key, val);
						alreadyAdded.add(key);
					}	
				}
			}
		}
	}
	
	private String getKey(String product1, String product2) {
		String key = product1.concat(SEPARATOR).concat(product2);
		if(product1.compareTo(product2) > 0)
			key = product2.concat(SEPARATOR).concat(product1);
		return key;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {			
		for(String key : itemCouples.keySet()) {
			String[] splitted = key.split(SEPARATOR);
			String outputKey = splitted[0].concat(OUT_SEPARATOR).concat(splitted[1]);
			context.write(new Text(outputKey), new IntWritable(itemCouples.get(key)));
		}
	}
	
}
