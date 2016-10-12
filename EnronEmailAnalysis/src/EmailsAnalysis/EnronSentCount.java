package EmailsAnalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.nio.file.FileSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EnronSentCount {
	/**
	 Map reduce program to determine the number of mails sent to each recipient
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
				
				Configuration conf = new Configuration();
				Job job = new Job(conf, "Enron_recipcount");
				job.setJarByClass(EnronSentCount.class);
				job.setMapperClass(mapping.class);
				job.setReducerClass(reducing.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				Path inputPath = new Path(args[0]);
				Path outputPath = new Path(args[1]);
				
				FileInputFormat.addInputPath(job, inputPath);
				FileOutputFormat.setOutputPath(job, outputPath);
			
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
			
			public static class mapping extends Mapper<LongWritable, Text, Text, Text>{
				int count = 0;
				@ Override
				public void map(LongWritable key1, Text val1, Context context) throws IOException, InterruptedException{
					
					int count = 0;
	/* Get the to and cc recipients to separate the data */				
					String str = val1.toString();
					int topos = str.indexOf("To:");
					int subjectpos = str.indexOf("Subject:");
					int ccpos = str.indexOf("X-cc:");
					int bccpos = str.indexOf("X-bcc:");

					String torecipients = str.substring(topos+4, subjectpos); //get To list
					String toccrecipients = str.substring(ccpos+6, bccpos); // get cc list
					
					String reciparr[] = torecipients.split(","); 
										
					for ( String x: reciparr){
					context.write(new Text(x.trim()), new Text("To"));
				}
					if (!toccrecipients.isEmpty()){
						String ccreciparr[] = toccrecipients.split(",");
						
					for ( String x: ccreciparr){
						context.write(new Text(x.trim()), new Text("cc"));
					}}
				}}
				
				public static class reducing extends Reducer<Text, Text, Text, DoubleWritable>{
					
					@ Override
					
					public void reduce(Text key1, Iterable<Text> val2, Context context) throws IOException, InterruptedException{
							
						Double recipcount =0.0;		
						// Add 1 for each time recipient appears in "To" list and 0.5 for "cc" list
						for (Text j: val2) {
							if (j.toString().equals("To")) recipcount += 1;
							else if (j.toString().equals("cc")) recipcount += 0.5;
							 }
						
						context.write(new Text(key1), new DoubleWritable(recipcount));
					}}
}
