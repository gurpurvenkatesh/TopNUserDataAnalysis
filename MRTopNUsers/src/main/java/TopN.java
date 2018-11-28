import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopN extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2)
			System.exit(2);
		
		conf.set("filter_n_records", "10");			/* setting configuration property */
		
		Job job = new Job(conf, "Top N");			/* used for job submitting, checking job state etc */
		job.setJarByClass(TopN.class);				/* This line let the data node to find for mapper & reducer in to this class */
		job.setMapperClass(TopMapper.class);		/* Setting Mapper class */	
		job.setReducerClass(TopNReducer.class);		/* Setting Reducer class */
		job.setNumReduceTasks(1);					/* Set number of reduce class */
		
		job.setMapOutputKeyClass(NullWritable.class);	/* Setting Mapper Output key class */
		job.setMapOutputValueClass(Text.class);			/* Setting Mapper Output Value class */
		
		job.setOutputKeyClass(Text.class);				/* Setting Final Output key class */
		job.setOutputValueClass(LongWritable.class);	/* Setting Final Output Value class */
		
		/* Input & Output path is sent through the command line argument */
		FileInputFormat.addInputPath(job, new Path(args[0])); /* default input format is TextInputFormat which extends FileInputFormat */	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		
		return (job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static void main(String args[]) throws Exception {
		/* trigger the job below */
	    int res = ToolRunner.run(new Configuration(), new TopN(), args); /* Execute the command with the given args command line arguments */
	    System.exit(res);
	}

}
