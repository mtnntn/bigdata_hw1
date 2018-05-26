package bd_ex1_gid.bd_ex1_aid.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bd_ex1_gid.bd_ex1_aid.Esercizio1;
import bd_ex1_gid.bd_ex1_aid.model.YearWordTuple;

public class MapperPhase extends Mapper<Object, Text, YearWordTuple, IntWritable>{

	public final static String TOKENS = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	public final static int YEAR_INDEX = 7;
	public final static int SUMMARY_INDEX = 8;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splittedLine = line.split(",");

		try {
			String summary = splittedLine[SUMMARY_INDEX].toLowerCase().replaceAll(TOKENS, " ");
			String year = splittedLine[YEAR_INDEX].split("-")[0];	

			StringTokenizer tokenizer = new StringTokenizer(summary);
			while (tokenizer.hasMoreTokens()) {
				YearWordTuple wordEntry = new YearWordTuple(year, tokenizer.nextToken());
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
