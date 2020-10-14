package reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * In the reducer we will calculate a simple average by summing up all the values passed
 * on from mapper.
 * Ex:
 * Key 			Values
 * "9999,Dan" 90, 78, 51, .....
 * 
 * We will do this for every user and output their average.
 * */
public class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	
	//Variables to write out to file.
	Text user_name = new Text(); 
	DoubleWritable friends_average_age = new DoubleWritable(); 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//Variables to calculate average
		int total_sum_of_friends = 0;
		int count = 0;
		
		//Summing up age of friends to get total
		for (IntWritable age_of_friend : values) {
			total_sum_of_friends += age_of_friend.get();
			count++;
		}
		
		//Computing friends average for a specific user
		double average_friends_age = (double)(total_sum_of_friends) / count;
		
		//parsing the key to only get the first name of the user.
		String[] user_id_and_name = key.toString().split(",");
		String user_first_name = user_id_and_name[1];
		
		//initializing variables for output
		user_name.set(user_first_name);
		friends_average_age.set(average_friends_age);

		//Output will be in the form of:
		//Dan	62.38461538461539
		context.write(user_name, friends_average_age);
	}
}