package bd_ex1_gid.bd_ex1_aid.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import bd_ex1_gid.bd_ex1_aid.Esercizio1;

public class ReducerPhase2 extends Reducer<Text, IntWritable, Text, Text> {

	private Map<String, Map<String, Integer>> countMap = new TreeMap<String, Map<String, Integer>>();

	@Override
	public void reduce(Text yearWord, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int frequencyForWord = 0;
		for (IntWritable val : values) {
			frequencyForWord += val.get();
		}
		
		String[] splitted = yearWord.toString().split("-");
		String year = splitted[0];
		String word = splitted[1];
		
		Map<String, Integer> wordsEntryForYear = countMap.get(year);
		if(wordsEntryForYear == null) {
			wordsEntryForYear = new HashMap<String, Integer>();				
		}
		wordsEntryForYear.put(word, frequencyForWord);	
		countMap.put(year, wordsEntryForYear);	
	
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		for(String year : countMap.keySet()){
			//System.out.println("\n\n---> " + year);
			int counter = 0;
			Map<String, Integer> wordsForTheYear = countMap.get(year);
			//Collections.sort(wordsForTheYear);
			for (String wordInCurrentYear : wordsForTheYear.keySet()) {
				if (counter++ > Esercizio1.TOP_N)
					break;
				String val = wordInCurrentYear + " " + wordsForTheYear.get(wordInCurrentYear);
				context.write(new Text(year), new Text(val));
			}
		}
	}
	
}
