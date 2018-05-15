package bd_ex2_gid.bd_ex2_aid.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

public class MyUtils {

	public static void printMap(Map<Text, List<Object>> m) {
		System.out.println("\n ----------------------- COMMON MAP --------------------------- \n");
//		for(Object yearKey : m.keySet()) {
//			System.out.println("\n ----> Year "+ yearKey +" \n");
//			for(Object w : m.get(yearKey))
//				System.out.println("\n ---------> Word: ( " + w + ", " + w.getFrequency() + ") ");
//			System.out.println("\n -------------------------------------------------------------- \n");
//		}
	}
	
	public static Text getYearFromDate(String unixDatetime) {
		 String year = new SimpleDateFormat("yyyy").format(new Date(Long.valueOf(unixDatetime) * 1000L));
		 return new Text(year);
	}
}
