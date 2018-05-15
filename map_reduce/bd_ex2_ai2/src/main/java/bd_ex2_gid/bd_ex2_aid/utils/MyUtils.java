package bd_ex2_gid.bd_ex2_aid.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;

import bd_ex2_gid.bd_ex2_aid.model.SumAverageTuple;

public class MyUtils {

	public static Text getYearFromDate(String unixDatetime) {
		 String year = new SimpleDateFormat("yyyy").format(new Date(Long.valueOf(unixDatetime) * 1000L));
		 return new Text(year);
	}
	
	public static void printMap(Map<String, Map<String, SumAverageTuple>> m) {
		System.out.println("\n ----------------------- MAP --------------------------- \n");
		for(String productId : m.keySet()) {
			System.out.println("\n ----> Product: "+ productId +" \n");
			for(String year : m.get(productId).keySet()) {
				System.out.println("\n ---------> Year: "+ year +" \n");
				SumAverageTuple w = m.get(productId).get(year);
				System.out.println("\n -------------> Average: "+ w.getAverage() +"\n");
			}
			System.out.println("\n -------------------------------------------------------------- \n");
		}
	}
}
