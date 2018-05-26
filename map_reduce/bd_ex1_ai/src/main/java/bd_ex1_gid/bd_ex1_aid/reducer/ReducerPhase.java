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
import bd_ex1_gid.bd_ex1_aid.model.YearWordTuple;

public class ReducerPhase extends Reducer<YearWordTuple, IntWritable, Text, WordFrequencyTuple> {

	private Map<String, List<WordFrequencyTuple>> countMap = new TreeMap<String, List<WordFrequencyTuple>>();

	@Override
	public void reduce(YearWordTuple wordKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

//		System.out.println("\n ------------------ \n");
//		System.out.println(" Analizing word: ( " + wordKey + ", " + wordKey.getFrequency() + ") ");

		String currentYear = wordKey.getYear().toString();
		String currentWord = wordKey.getWord().toString();
		WordFrequencyTuple currentWordEntry = new WordFrequencyTuple(currentWord);
		
		for (IntWritable val : values) {
			currentWordEntry.increaseFrequency(val.get());
		}
		
//		System.out.println(" New Frequency for word: ( " + currentWordEntry + " " + currentWordEntry.getFrequency() + ") ");
//		System.out.println("\n ------------------ \n");

		List<WordFrequencyTuple> wordsEntryForYear = countMap.get(currentYear);
		if(wordsEntryForYear == null) {
			wordsEntryForYear = new ArrayList<WordFrequencyTuple>();				
		}
		wordsEntryForYear.add(currentWordEntry);	
//		System.out.println("\n ------------------ Words for the year -------------------- \n");
//		for(WordEntry w : wordsEntryForYear)
//			System.out.println("\n -----> Word: ( " + w + ", " + w.frequency + ") ");
//		System.out.println("\n ----------------------------------------------------------- \n");		
		countMap.put(currentYear, wordsEntryForYear);	
	
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		
		for(String year : countMap.keySet()){
			int counter = 0;
			List<WordFrequencyTuple> wordsForTheYear = countMap.get(year);
			Collections.sort(wordsForTheYear);
			for (WordFrequencyTuple wordInCurrentYear : wordsForTheYear) {
				if (counter++ == Esercizio1.TOP_N) {
					break;
				}
				context.write(new Text(year), wordInCurrentYear);
			}
		}
	}
	
}
