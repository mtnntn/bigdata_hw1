package bd_ex1_gid.bd_ex1_aid.utils;

import java.util.List;
import java.util.Map;

import bd_ex1_gid.bd_ex1_aid.model.WordFrequencyTuple;

public class MyUtils {

	public static void printMap(Map<String, List<WordFrequencyTuple>> m) {
		System.out.println("\n ----------------------- COMMON MAP --------------------------- \n");
		for(String yearKey : m.keySet()) {
			System.out.println("\n ----> Year "+ yearKey +" \n");
			for(WordFrequencyTuple w : m.get(yearKey))
				System.out.println("\n ---------> Word: ( " + w + ") ");
			System.out.println("\n -------------------------------------------------------------- \n");
		}
	}
}
