package bd_ex1_gid.bd_ex1_aid.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import bd_ex1_gid.bd_ex1_aid.Esercizio1;
import bd_ex1_gid.bd_ex1_aid.model.WordFrequencyTuple;

public class ReducerPhase extends Reducer<Text, IntWritable, Text, Text> {

	private Map<String, List<WordFrequencyTuple>> countMap = new TreeMap<String, List<WordFrequencyTuple>>();

	@Override
	public void reduce(Text yearWord, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int frequencyForWord = 0;
		for (IntWritable val : values) {
			frequencyForWord += val.get();
		}
		
		String[] splitted = yearWord.toString().split("-");
		String year = splitted[0];
		String word = splitted[1];
		
//		System.out.println("KEY : " + yearWord);
//		System.out.println("YEAR: " + year);
//		System.out.println("WORD: " + word);
//		System.out.println("FREQ: " + frequencyForWord);
		
		List<WordFrequencyTuple> wordsEntryForYear = countMap.get(year);
		if(wordsEntryForYear == null) {
			wordsEntryForYear = new ArrayList<WordFrequencyTuple>();				
		}
		wordsEntryForYear.add(new WordFrequencyTuple(word, frequencyForWord));	
		countMap.put(year, wordsEntryForYear);	
	
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		for(String year : countMap.keySet()){
			//System.out.println("\n\n---> " + year);
			int counter = 0;
			List<WordFrequencyTuple> wordsForTheYear = countMap.get(year);
			Collections.sort(wordsForTheYear);
			for (WordFrequencyTuple wordInCurrentYear : wordsForTheYear) {
				if (counter++ > Esercizio1.TOP_N)
					break;
				context.write(new Text(year), new Text(wordInCurrentYear.toString()));
				//System.out.println("------> " + wordInCurrentYear.toString());
				 
			}
		}
	}
	
}
