package driver;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapper.Map;
import reducer.Reduce;

public class MutualFriendsList {

	public static void main(String[] args) throws Exception{
		
		//Checking command line arguments are passed correctly
		if(args.length!=2) {
			System.out.println("Use the following command line arguments: <input_file> <output_path>");
			System.exit(1);
		}
		
		
		//Setting up the job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MutualFriendsList");

		//Setting up jar file name
		job.setJarByClass(MutualFriendsList.class);
		//Setting up map class name
		job.setMapperClass(Map.class);
		//Setting up reduce class name
		job.setReducerClass(Reduce.class);

		//setting up key values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//HDFS file input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//HDFS output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
