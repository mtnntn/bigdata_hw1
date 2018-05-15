package bd_ex3_gid.bd_ex3_aid.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

public class MyUtils {

	public static Text getYearFromDate(String unixDatetime) {
		 String year = new SimpleDateFormat("yyyy").format(new Date(Long.valueOf(unixDatetime) * 1000L));
		 return new Text(year);
	}
	
	public static void printMap(Map<String, List<String>> m, String mapName) {
		System.out.println("\n ----------------------- MAP: "+ mapName +" --------------------------- \n");
		for(String k : m.keySet()) {
			System.out.println("\n ----> "+ k);
			for(String l : m.get(k)) {
				System.out.println("\n --------> "+ l);
			}
			System.out.println("\n");
		}
		System.out.println("\n ---------------------------------------------------------------------- \n");
	}
}
