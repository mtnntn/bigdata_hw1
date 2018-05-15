package bd_ex2_gid.bd_ex2_aid.reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import bd_ex2_gid.bd_ex2_aid.Esercizio2;
import bd_ex2_gid.bd_ex2_aid.model.SumAverageTuple;
import bd_ex2_gid.bd_ex2_aid.model.YearScoreTuple;

public class ReducerPhase extends Reducer<Text, YearScoreTuple, Text, Text> {

	//productId -> { year -> SumAverageTuple}
	private Map<String, Map<String, SumAverageTuple>> result = new TreeMap<String, Map<String, SumAverageTuple>>();
	
	@Override
	public void reduce(Text productId, Iterable<YearScoreTuple> values, Context context) throws IOException, InterruptedException {
		Map<String, SumAverageTuple> productScoresPerYear = new TreeMap<String, SumAverageTuple> ();
		for(YearScoreTuple yearScore : values) {
			SumAverageTuple sumAvgTuple = productScoresPerYear.get(yearScore.getYear().toString());
			if(sumAvgTuple == null)
				sumAvgTuple = new SumAverageTuple(yearScore.getScore());
			else
				sumAvgTuple.addNewValue(yearScore.getScore());
			productScoresPerYear.put(yearScore.getYear().toString(), sumAvgTuple);
		}
		
		int maxYear = Integer.valueOf(Esercizio2.MAX_YEAR.toString());
		int minYear = Integer.valueOf(Esercizio2.MIN_YEAR.toString());
		
		while(maxYear >= minYear) {
			String currentYearString = String.valueOf(maxYear);
			if(! productScoresPerYear.containsKey(currentYearString)) {
				productScoresPerYear.put(currentYearString, new SumAverageTuple());
			}
			maxYear -= 1;
		}
		result.put(productId.toString(), productScoresPerYear);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		for(String productId : result.keySet()) {
			String acc = "";
			Map<String, SumAverageTuple> meansForYears = result.get(productId);
			for(String year : meansForYears.keySet()) {
				SumAverageTuple yearAverage = meansForYears.get(year);
				acc = acc.concat(yearAverage.getAverage() + "\t");
			}
			context.write(new Text(productId), new Text(acc));
		}
	}


}
