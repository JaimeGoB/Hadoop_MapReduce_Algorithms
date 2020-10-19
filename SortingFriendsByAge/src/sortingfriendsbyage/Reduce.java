package sortingfriendsbyage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<UserFriendAgePair, Text, IntWritable,Text>{
    
	IntWritable userID = new IntWritable();
	
    protected void reduce(UserFriendAgePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    	//Setting up userID
    	userID.set(key.userId);
    	
    	//Values will contain the friend first name and age format
    	for(Text friend_name_age:values){
        	//Writing out the key with sorted list per friend.
            context.write(  userID , friend_name_age);
        }
    }
}
