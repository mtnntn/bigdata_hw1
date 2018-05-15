package bd_ex1_gid.bd_ex1_aid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bd_ex1_gid.bd_ex1_aid.mapper.MapperPhase;
import bd_ex1_gid.bd_ex1_aid.reducer.ReducerPhase;

/**
 * Il JOB deve generare, per ciascun anno, le dieci parole che sono state più usate
 * nelle recensioni (campo summary) in ordine di frequenza.
 * Bisogna indicare, per ogni parola, la sua frequenza:
 * ovvero il numero di occorrenze della parola nelle recensioni di quell’anno.  
 **/
public class Esercizio1 {

	public final static IntWritable ONE = new IntWritable(1);
	public final static String TOKENS = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	public final static int YEAR_INDEX = 7;
	public final static int REVIEW_INDEX = 9;
	public final static int TOP_N = 10;
	
	public static void main(String[] args) throws Exception {
		System.out.println("\n\n PARTENZA!!!!");
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Job job = new Job(conf, "Esercizio1");
		
		Path output = new Path(args[1]);
		Path input  = new Path(args[0]);
		
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		
		job.setJarByClass(Esercizio1.class);

		job.setMapperClass(MapperPhase.class);
		job.setReducerClass(ReducerPhase.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		System.out.println("\n\n Finisco!!!!");
		job.waitForCompletion(true);

	}

}

