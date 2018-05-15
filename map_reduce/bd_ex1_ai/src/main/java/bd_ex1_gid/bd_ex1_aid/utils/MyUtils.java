package bd_ex1_gid.bd_ex1_aid.utils;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import bd_ex1_gid.bd_ex1_aid.model.YearWordTuple;

public class MyUtils {

	public static void printMap(Map<Text, List<YearWordTuple>> m) {
		System.out.println("\n ----------------------- COMMON MAP --------------------------- \n");
		for(Text yearKey : m.keySet()) {
			System.out.println("\n ----> Year "+ yearKey +" \n");
			for(YearWordTuple w : m.get(yearKey))
				System.out.println("\n ---------> Word: ( " + w + ", " + w.getFrequency() + ") ");
			System.out.println("\n -------------------------------------------------------------- \n");
		}
	}
}
