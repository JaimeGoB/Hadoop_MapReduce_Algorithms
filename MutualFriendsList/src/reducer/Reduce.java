package reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * The list is received like:
 * (777,999) "111,222,777" , "222,888,999"
 * 
 * Need to output:
 * (777,999) 222
 * 
 * */
public class Reduce extends Reducer<Text,Text,Text,Text> {

	public void reduce (Text pairKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//Creating hashmap to store friends in common from two different friends
		HashMap<String, Integer> all_friends_from_pair_of_friends = new HashMap<String,Integer>();
		
		
		//initializing Text to store final string of common friends
		Text commonFriends = new Text();
		
		StringBuilder commonFriendBuilder = new StringBuilder();
		
		for (Text friends : values) {
			
			//Separating the string separated by , by split ","
			String[] friendsList = friends.toString().split(",");
			
			//Since we have two lists, we need to iterate through both lists to we find mutual friends
			for (String friend : friendsList) {
				
				//checking if the friend is already on the hashmap
				if(all_friends_from_pair_of_friends.containsKey(friend)){
					
					//If the friend is already on the map, this means that 
					//they share a mutual friend
					//Thus we add values to string builder
					commonFriendBuilder.append(friend+",");
				}else {
					//if the friend does not exist in the hashmap we will add it
					/* all_friends will look like:
					 * 111
					 * 222
					 * 777
					 * 888
					 * 999
					 * */
					all_friends_from_pair_of_friends.put(friend, 1);
				}
			}
		}
		
		//ONLY OUTPUT friends with mutual friends
		//Will NOT output empty lists
		if(commonFriendBuilder.length() > 0 && commonFriendBuilder != null) {
			
			//delete last comma from string builder
			commonFriendBuilder.deleteCharAt(commonFriendBuilder.length()-1);
			//converting the string builder to string and storing it in text for key val pair
			// 222
			commonFriends.set(new Text(commonFriendBuilder.toString()));
			
			//           (777,999), 222
			context.write(pairKey, commonFriends);
		}
	}
}


