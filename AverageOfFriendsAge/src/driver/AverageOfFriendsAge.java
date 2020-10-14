package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapper.Map;
import reducer.Reduce;


//The args are: file_path_userdata 	input_file 	output folder
public class AverageOfFriendsAge {

	public static void main(String[] args) throws Exception {
		
		//Checking command line arguments are passed correctly
		if(args.length!=3) {
			System.out.println("Use the following command line arguments: <in_memory_input_file> <input_file> <output_path>");
			System.exit(1);
		}
				
	    //Setting up configuration (setting up filepath for file to be read in memory)
		Configuration conf = new Configuration();
		conf.set("file_path_for_userdata", args[0]);
		
		//Creating new job
		Job job = Job.getInstance(conf, "AverageOfFriendsAge");
		
		//Setting up jar file name
		job.setJarByClass(AverageOfFriendsAge.class);
		
		//Setting up map class name
		job.setMapperClass(Map.class);
		//setting up key values for reducer output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
		
		//Setting up reduce class name
		job.setReducerClass(Reduce.class);
		//setting up key values for reducer output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		//HDFS file input path
		FileInputFormat.addInputPath(job, new Path(args[1]));
		//HDFS output path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
