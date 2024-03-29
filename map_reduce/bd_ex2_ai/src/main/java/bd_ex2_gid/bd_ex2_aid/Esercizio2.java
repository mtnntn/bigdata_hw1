package bd_ex2_gid.bd_ex2_aid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bd_ex2_gid.bd_ex2_aid.mapper.MapperPhase;
import bd_ex2_gid.bd_ex2_aid.model.ProductYearTuple;
import bd_ex2_gid.bd_ex2_aid.model.SumAverageTuple;
import bd_ex2_gid.bd_ex2_aid.reducer.ReducerPhase;

public class Esercizio2 {

	/**
	 * Un job che sia in grado di generare, per ciascun prodotto, lo score medio ottenuto in ciascuno 
	 * degli anni compresi tra il 2003 e il 2012.
	 * Bisogna indicare ProductId seguito da tutti gli score medi ottenuti negli anni dell’intervallo. 
	 * Il risultato deve essere ordinato in base al ProductId.  
	 *
	 * Voglio un output del tipo: 
	 * 
	 * prod_id mean2003 mean2004 mean2005 mean2005 mean2006 mean2007 mean2008 mean2009 mean2010 mean2011 mean2012  
	 * 		 1		3.4		 2.4	  3.4	   2.4		3.4		 2.4	  3.4	   2.4		3.4		 2.4	  3.4	     
	 * 		 2		3.4		 0.0	  0.0	   0.0		2.4		 0.0	  0.0	   4.4		0.0		 0.0	  0.0	     
	 * 
	 * prod_id year_x mean_year_x
	 *	1		2003		3.4		 
	 *	1		2006		2.4	  
	 *	1		2010		3.4	   
	 * 		 
	 **/
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Job job = new Job(conf, "Esercizio2");
		
		Path output = new Path(args[1]);
		Path input  = new Path(args[0]);
		
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		
		job.setJarByClass(Esercizio2.class);

		job.setMapperClass(MapperPhase.class);
		job.setReducerClass(ReducerPhase.class);
		
		job.setOutputKeyClass(ProductYearTuple.class);
		job.setOutputValueClass(SumAverageTuple.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
	
}
