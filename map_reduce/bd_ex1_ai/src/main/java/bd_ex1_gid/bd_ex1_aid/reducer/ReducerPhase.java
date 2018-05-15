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
import bd_ex1_gid.bd_ex1_aid.model.YearWordTuple;

public class ReducerPhase extends Reducer<YearWordTuple, IntWritable, YearWordTuple, IntWritable> {

	private Map<Text, List<YearWordTuple>> countMap = new TreeMap<Text, List<YearWordTuple>>();

	@Override
	public void reduce(YearWordTuple wordKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

//		System.out.println("\n ------------------ \n");
//		System.out.println(" Analizing word: ( " + wordKey + ", " + wordKey.getFrequency() + ") ");

		IntWritable frequencyForWord = new IntWritable(0);
		for (IntWritable val : values) {
			frequencyForWord.set(frequencyForWord.get() + val.get());
		}
		YearWordTuple currentWordEntry = new YearWordTuple(wordKey.getYear(), wordKey.getWord(), frequencyForWord);

//		System.out.println(" New Frequency for word: ( " + currentWordEntry + " " + currentWordEntry.getFrequency() + ") ");
//		System.out.println("\n ------------------ \n");

		List<YearWordTuple> wordsEntryForYear = countMap.get(currentWordEntry.getYear());
		if(wordsEntryForYear == null) {
			wordsEntryForYear = new ArrayList<YearWordTuple>();				
		}
		wordsEntryForYear.add(currentWordEntry);	
//		System.out.println("\n ------------------ Words for the year -------------------- \n");
//		for(WordEntry w : wordsEntryForYear)
//			System.out.println("\n -----> Word: ( " + w + ", " + w.frequency + ") ");
//		System.out.println("\n ----------------------------------------------------------- \n");		
		countMap.put(currentWordEntry.getYear(), wordsEntryForYear);	
	
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		
		for(Text year : countMap.keySet()){
			int counter = 0;
			List<YearWordTuple> wordsForTheYear = countMap.get(year);
			Collections.sort(wordsForTheYear);
			for (YearWordTuple wordInCurrentYear : wordsForTheYear) {
				if (counter++ == Esercizio1.TOP_N) {
					break;
				}
				context.write(wordInCurrentYear, wordInCurrentYear.getFrequency());
			}
		}
	}
	
}
