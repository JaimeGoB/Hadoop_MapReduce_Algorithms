package sortingfriendsbyage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/*
* Hashing over userid to make sure that objects with same userID go to same reducer
*/
public class UserFriendAgePartitioner extends Partitioner<UserFriendAgePair, Text> {

	public int getPartition(UserFriendAgePair  pair, Text val, int numberOfPartitions) {
		return Math.abs(pair.hashCode() % numberOfPartitions);
	}
}
