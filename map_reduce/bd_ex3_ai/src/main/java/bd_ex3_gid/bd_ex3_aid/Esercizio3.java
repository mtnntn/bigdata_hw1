package bd_ex3_gid.bd_ex3_aid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bd_ex3_gid.bd_ex3_aid.mapper.MapperPhase;
import bd_ex3_gid.bd_ex3_aid.reducer.ReducerPhase;

public class Esercizio3 {

	/**
	 * Un job in grado di generare coppie di prodotti che hanno almeno un utente in comune.
	 * Questo significa dover trovare coppie di prodotti che sono state stati recensiti da uno stesso utente.
	 * Bisogna indicare, per ciascuna coppia, il numero di utenti in comune. 
	 * Il risultato deve essere ordinato in base al ProductId del primo elemento della coppia e, possibilmente, 
	 * non deve presentare duplicati.   
	 **/
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Job job = new Job(conf, "Esercizio3");
		
		Path output = new Path(args[1]);
		Path input  = new Path(args[0]);
		
		if (hdfs.exists(output)) {
		    hdfs.delete(output, true);
		}
		
		job.setJarByClass(Esercizio3.class);

		job.setMapperClass(MapperPhase.class);
		job.setReducerClass(ReducerPhase.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
		
	}
	
}



