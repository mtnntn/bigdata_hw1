package bd_ex2_gid.bd_ex2_aid.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import bd_ex2_gid.bd_ex2_aid.model.ProductYearTuple;
import bd_ex2_gid.bd_ex2_aid.model.SumAverageTuple;

public class ReducerPhase extends Reducer<ProductYearTuple, SumAverageTuple, ProductYearTuple, DoubleWritable> {

	private SumAverageTuple result = new SumAverageTuple();
	
	@Override
	public void reduce(ProductYearTuple productEntry, Iterable<SumAverageTuple> values, Context context) throws IOException, InterruptedException {
		for(SumAverageTuple sumAvg : values) {
			result.addNewValue(sumAvg.getAverage());
		}
		context.write(productEntry, result.getAverage());
	}
}
