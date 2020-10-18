package sortingfriendsbyage;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class Map extends Mapper<LongWritable, Text, UserFriendAgePair, Text>{
   
	//Key- UserId and Friends Age object with IntWritables
	IntWritable userId = new IntWritable();
	IntWritable friends_age = new IntWritable();
	
	//Value for object - containing first name and age of friend
	Text friends_first_name_And_age = new Text();

	/*
	 * Creating hashmap to store user information loading in memory
	 * in a hashmap:
	 * 
	 * KEY(string)		 VALUE(string)
	 * userid  	        user_age,user_first_name
	 * "0"				"24,Evangeline"
	 * "1"				"47,Robert"
	 * "2"				"70,Kathyrn"
	 * "3" 				"85,Paula"
	 * ..............
	 * */
    static HashMap<String, String> lookup_table_user_information;
    
    //Load file in memory and store only useful information into lookup table
    //We only need: USERID, AGE AND FNAME
    public void setup(Context context) throws IOException{
		
    	//copying configuration from context object to config object
    	Configuration config = context.getConfiguration();
    	
		//Initializing lookup table (hashmap)
    	lookup_table_user_information = new HashMap<String, String>();
		
    	//getting the file path stored in driver
    	String file_path_for_userdata = config.get("file_path_for_userdata");
		
    	//getting full path of userdata in hdfs
    	Path path_for_user_data = new Path("hdfs://localhost:9000" +file_path_for_userdata);
		
    	//accessing filesystem of config
    	FileSystem fileSystem = FileSystem.get(config);
    	
    	//reading first line of user data into buffer
		BufferedReader line_from_buffered_reader = new BufferedReader(new InputStreamReader(fileSystem.open(path_for_user_data)));
		
		//converting first line of buffer to string
		String line_from_userdata_file = line_from_buffered_reader.readLine();
		
		//loop until we reach EOF
		while (line_from_userdata_file != null) {
			
			//0,Evangeline,Taylor,3396 Rogers Street,Loveland,Ohio,45140,US,Unfue1996,1/24/1996
			//0|	1	  |  2	 |		3			|   4	 | 5  |  6  |7 |    8	 |   9
			String[] all_user_information = line_from_userdata_file.split(",");
			
			//We will only read users that have all information 
			if (all_user_information.length == 10) {
				
				/*
				 * Storing relevant information from user:
				 * We need userid   first name     dob
				 * 
				 *user_info ["0" | "Evangeline" | "1/24/1996"]
				 *index       0        1               9
				 * */
				String user_id = all_user_information[0];
				String user_first_name = all_user_information[1];
				String user_date_of_birth = get_age(all_user_information[9]);
				
				//concatenating two strings
				String user_info = user_date_of_birth + "," + user_first_name;
				
				//Storing key value pair in lookup table
				//								   "0"  "24,Evangeline"
				lookup_table_user_information.put(user_id, user_info);
			}
			//Move to next line
			line_from_userdata_file = line_from_buffered_reader.readLine();
		}
    }
		
	//Function to calculate age
	String get_age(String dob)
	{
		//Splitting dob by /
		String[] dobFields = dob.split("/");
		//Getting year from month, day, year
		String year =  dobFields[2];
		//Getting age of user
		int age = 2020 - Integer.parseInt(year);
		//converting age to string
		String age_str = Integer.toString(age);
		
		return age_str;
	}
	
	
	/*
	 * The output of mapper will be:
	 * 
	 * KEY(object)		  		VALUE(int)
	 * 
	 * UserFriendAgePair		friends_age
	 *  0, 24					"Robert,24"
	 *  0, 47					"Kathyrn,47"
	 *  0, 70					"Harry,70"
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//split the line by a tab
		//lineInFile [ 0 | 1,2,3,4,....]
		//lineInFile[0] is ID and [1] is FRIENDS of ID
		String[] lineInFile = value.toString().split("\\t");
		
		//Check that a user has fiends otherwise move on to next line
		if( lineInFile.length == 1 ) return;
			
		//User has friends, so now we store friends from user into an array
		//[ 1 | 2 | 3 | 4 |....]
		String[] friends_array = lineInFile[1].split(",");
		String userid = lineInFile[0];
		userId.set(Integer.parseInt(userid));

		//Iterate through friends of user id to get the ages of all friends
		for (String friend : friends_array) {
			
			
			//Key 1 will give value of "24,Robert"
			String[] friend_age_and_name = lookup_table_user_information.get(friend).split(",");
			
			//Getting the friends age. In this case getting age of user 1 not user 0.
			// friend_age = 24
			int friend_age = Integer.parseInt(friend_age_and_name[0]);
			//Getting friends first name
			// friend_name = Robert
			String friend_name = friend_age_and_name[1];
			//Use to store friends age as IntWritable in Pair Object
			friends_age.set(friend_age);
			
			//Setting up key
			UserFriendAgePair userId_with_FriendsAge_Key = new UserFriendAgePair(userId, friends_age);
			//Setting up Value
			friends_first_name_And_age.set(friend_name + " \t"+ friend_age);

			
			// UserFriendAgePair Followed by the friends age
			// * reducerKey					val
			// * IntWritables (both)		Text
			// *   0 , 24				"Robert 24"
			// *   0 , 47				"Kathryn 47"
			// ............
			context.write(userId_with_FriendsAge_Key, friends_first_name_And_age); 
		}
	}
}