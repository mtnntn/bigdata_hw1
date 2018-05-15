package bd_ex1_gid.bd_ex1_aid.mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bd_ex1_gid.bd_ex1_aid.Esercizio1;
import bd_ex1_gid.bd_ex1_aid.model.YearWordTuple;

public class MapperPhase extends Mapper<LongWritable, Text, YearWordTuple, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splittedLine = line.split(",");

		try {
			String review = splittedLine[splittedLine.length-1].toLowerCase().replaceAll(Esercizio1.TOKENS, " ");
			String year = splittedLine[Esercizio1.YEAR_INDEX];	
			year = new SimpleDateFormat("yyyy").format(new Date(Long.valueOf(year) * 1000L));

			StringTokenizer tokenizer = new StringTokenizer(review);
			while (tokenizer.hasMoreTokens()) {
				YearWordTuple wordEntry = new YearWordTuple(new Text(year), new Text(tokenizer.nextToken()));
				context.write(wordEntry, Esercizio1.ONE);
			}
		}
		catch (Exception e) {
			System.out.println("\n ------------------ \n");
			System.out.println(" Error reading line: " + line);
			System.out.println("\n ------------------ \n");
		}

	}
	
}
