package bd_ex1_gid.bd_ex1_aid.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bd_ex1_gid.bd_ex1_aid.Esercizio1;

public class MapperPhase extends Mapper<Object, Text, Text, IntWritable>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splittedLine = line.split(",");

		try {
			String review = splittedLine[Esercizio1.REVIEW_INDEX].toLowerCase().replaceAll(Esercizio1.TOKENS, " ");
			String year = splittedLine[Esercizio1.YEAR_INDEX];	
			year = new SimpleDateFormat("yyyy").format(new Date(Long.valueOf(year) * 1000L));

			StringTokenizer tokenizer = new StringTokenizer(review);
			while (tokenizer.hasMoreTokens()) {
				Text yearWordKey = new Text(year.concat("-").concat(tokenizer.nextToken()));
				context.write(yearWordKey, Esercizio1.ONE);
			}
		}
		catch (Exception e) {
			System.out.println("\n ------------------ \n");
			System.out.println(" Error reading line: " + line);
			System.out.println("\n ------------------ \n");
		}

	}
	
}
