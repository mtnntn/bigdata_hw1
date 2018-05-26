package bd_ex3_gid.bd_ex3_aid.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPhase extends Mapper<Object, Text, Text, Text>{

	private final static int PRODUCTID_INDEX = 1;
	private final static int USERID_INDEX = 2;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splittedLine = line.split(",");
		try {
			String productID = splittedLine[PRODUCTID_INDEX];	
			String userID = splittedLine[USERID_INDEX];	
			context.write(new Text(userID), new Text(productID));
		}
		catch (Exception e) {
			System.out.println("\n ------------------ \n");
			System.out.println(" Error reading line: " + line);
			System.out.print(e.toString());
			System.out.println("\n ------------------ \n");
		}
	}
	
}
