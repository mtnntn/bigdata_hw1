package bd_ex2_gid.bd_ex2_aid.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bd_ex2_gid.bd_ex2_aid.model.ProductYearTuple;
import bd_ex2_gid.bd_ex2_aid.model.SumAverageTuple;

public class MapperPhase extends Mapper<Object, Text, ProductYearTuple, SumAverageTuple>{

	private final static int PRODUCT_ID_INDEX = 1;
	private final static int REVIEW_SCORE_INDEX = 6;
	private final static int REVIEW_TIME_INDEX = 7;
	private final static Text MIN_YEAR = new Text("2003");
	private final static Text MAX_YEAR = new Text("2014");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splittedLine = line.split(",");
		try {
			Text textYear = this.getYearFromDate(splittedLine[REVIEW_TIME_INDEX]);
			if(textYear.compareTo(MIN_YEAR) >= 0 && textYear.compareTo(MAX_YEAR) <= 0) {
				Text productId = new Text(splittedLine[PRODUCT_ID_INDEX]);
				DoubleWritable reviewScore = new DoubleWritable(Double.valueOf(splittedLine[REVIEW_SCORE_INDEX]));
				ProductYearTuple productEntry = new ProductYearTuple(textYear, productId);
				SumAverageTuple sumAvgTuple = new SumAverageTuple(reviewScore);
				context.write(productEntry, sumAvgTuple);
			}
		}
		catch (Exception e) {
			System.out.println("\n ------------------ \n");
			System.out.println(" Error reading line: " + line);
			System.out.println("\n ------------------ \n");
		}
	}
	
	private Text getYearFromDate(String yyyyMMdd) {
		 //String year = new SimpleDateFormat("yyyy").format(new Date(Long.valueOf(unixDatetime) * 1000L));
		 return new Text(yyyyMMdd.split("-")[0]);
	}
}
