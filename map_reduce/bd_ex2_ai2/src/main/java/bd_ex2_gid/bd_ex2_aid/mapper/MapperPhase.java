package bd_ex2_gid.bd_ex2_aid.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bd_ex2_gid.bd_ex2_aid.Esercizio2;
import bd_ex2_gid.bd_ex2_aid.model.YearScoreTuple;
import bd_ex2_gid.bd_ex2_aid.utils.MyUtils;

public class MapperPhase extends Mapper<Object, Text, Text, YearScoreTuple>{

	private final static int PRODUCT_ID_INDEX = 1;
	private final static int REVIEW_SCORE_INDEX = 6;
	private final static int REVIEW_TIME_INDEX = 7;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splittedLine = line.split(",");
		try {
			Text textYear = MyUtils.getYearFromDate(splittedLine[REVIEW_TIME_INDEX]);
			if(textYear.compareTo(Esercizio2.MIN_YEAR) >= 0 && textYear.compareTo(Esercizio2.MAX_YEAR) <= 0) {
				Text productId = new Text(splittedLine[PRODUCT_ID_INDEX]);
				DoubleWritable reviewScore = new DoubleWritable(Double.valueOf(splittedLine[REVIEW_SCORE_INDEX]));
				YearScoreTuple productEntry = new YearScoreTuple(textYear, reviewScore);
				context.write(productId, productEntry);
			}
		}
		catch (Exception e) {
			System.out.println("\n ------------------ \n");
			System.out.println(" Error reading line: " + line);
			System.out.print(e.toString());
			System.out.println("\n ------------------ \n");
		}
	}
	
}
