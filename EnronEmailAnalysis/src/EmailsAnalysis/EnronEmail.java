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

public class EnronEmail {
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
				
		
				Configuration conf = new Configuration();
				Job job = new Job(conf, "Enron_email");
				job.setJarByClass(EnronEmail.class);
				job.setMapperClass(mapping.class);
				job.setReducerClass(reducing.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				
//				MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, mapping.class);
//				MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, txnmap.class);
				Path inputPath = new Path(args[0]);
				Path outputPath = new Path(args[1]);
				
				FileInputFormat.addInputPath(job, inputPath);
				FileOutputFormat.setOutputPath(job, outputPath);
			
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
			
			public static class mapping extends Mapper<LongWritable, Text, Text, IntWritable>{
				int count = 0;
				@ Override
				public void map(LongWritable key1, Text val1, Context context) throws IOException, InterruptedException{
					
					int count = 0;
	/* Get the message ID, From, to and email text start and end positions to separate the data */				
					String str = val1.toString();
					int msgidpos = str.indexOf("Message-ID:");
					int datepos = str.indexOf("Date:");
					int frompos = str.indexOf("From:");
					int topos = str.indexOf("To:");
					int subjectpos = str.indexOf("Subject:");
					int contentpos = str.indexOf("X-FileName:");
					String msgid = str.substring(msgidpos, datepos);
					String from = str.substring(frompos, topos);
					String to = str.substring(topos, subjectpos);
					
					String content = str.substring(contentpos);
					int contentactualstart = content.indexOf(".pst"); //actual content starts after .nsf or .pst filename
					if (contentactualstart == -1) contentactualstart = content.indexOf(".nsf");
					String contentactual = content.substring(contentactualstart+5);
					String tokenarray[] = contentactual.split(" ");
					count = tokenarray.length;
					
					context.write(new Text("Number of words to be averaged"), new IntWritable(count));
				}}
				
				public static class reducing extends Reducer<Text, IntWritable, Text, IntWritable>{
					
					@ Override
					
					public void reduce(Text key1, Iterable<IntWritable> val2, Context context) throws IOException, InterruptedException{
							
						int val = 0, mailcount =0;		
						int avgval = 0;
						for (IntWritable j: val2) {
							mailcount ++;
							 val += j.get();}
						
						if (mailcount>0) avgval = val/mailcount;
						
						context.write(new Text("Average number of words per e-mail"), new IntWritable(avgval));
					}}
		
}
