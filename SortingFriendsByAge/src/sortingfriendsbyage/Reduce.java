package sortingfriendsbyage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<UserFriendAgePair, Text, Text,Text>{
    
	
    protected void reduce(UserFriendAgePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	//String to attach list of sorted friends 
    	StringBuilder sorted_friend_and_age_list = new StringBuilder();

    	//Values will contain the friend first name and age format
    	for(Text friend_name_age:values){
        	
    		//Adding comma to seperat different friends
            sorted_friend_and_age_list.append(friend_name_age);
            sorted_friend_and_age_list.append(",");
            

        }
    	//Writing out the key with sorted list per friend.
        context.write( new Text(key.userId.toString()) ,new Text(sorted_friend_and_age_list.toString()));
    }
}
