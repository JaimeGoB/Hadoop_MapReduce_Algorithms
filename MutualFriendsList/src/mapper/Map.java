package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Example of first two lines from file input
 * 0	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94
 * 1	0,5,20,135,2409,8715,8932,10623,12347,12846,13840,13845,14005,20075,21556,22939,23520,28193,29724,29791,29826,30691,31232,31435,32317,32489,34394,35589,35605,35606,35613,35633,35648,35678,38737,43447,44846,44887,49226,49985,623,629,4999,6156,13912,14248,15190,17636,19217,20074,27536,29481,29726,29767,30257,33060,34250,34280,34392,34406,34418,34420,34439,34450,34651,45054,49592
 * 
 * */
public class Map extends Mapper<LongWritable, Text, Text, Text> {

	Text user = new Text();
	Text friends = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		//split the line by a tab
		//lineInFile [ 0 | 1,2,3,4,....]
		//lineInFile[0] is ID and [1] is FRIENDS of ID
		String[] lineInFile = value.toString().split("\\t");

		//Store userid into int variable
		int userid = Integer.parseInt(lineInFile[0]);
		
		//Check that a user has fiends otherwise move on to next line
		if( lineInFile.length == 1 ) return;
		
		//User has friends, so now we store friends from user into an array
		//[ 1 | 2 | 3 | 4 |....]
		String[] friends_array = lineInFile[1].split(",");
		
		//Iterate through array of friends
		for( String friend_iterator : friends_array ) {
		
			int friend = Integer.parseInt(friend_iterator);
			
			//Move to next value if user is friends with itself
		    if(userid == friend) continue;
		
		    //This will put the smaller value first 
		    /*
		     * Case 2:
		     * (222,777)
		     * 
		     * Case 1:
		     * (777,888)
		     * */
		    String Key = (userid < friend ) ? userid + "," + friend : friend + "," + userid;
		    
		    //Setting up key value pairs
		    friends.set(lineInFile[1].replaceAll("\\t|\\r\\n|\\r|\\n", ""));
		    user.set(Key);
		    
		    //writing out key value pairs to reducer
		    /*
		     * (222,777) friends list ....
		     * (777,888) friends list ...
		     * (777,999) ...
		     * */
		    context.write(user, friends);
		}
	}
}