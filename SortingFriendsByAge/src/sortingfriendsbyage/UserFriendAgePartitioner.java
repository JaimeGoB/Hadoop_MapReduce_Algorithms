package sortingfriendsbyage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class UserFriendAgePartitioner extends Partitioner<UserFriendAgePair, Text> {

	public int getPartition(UserFriendAgePair  pair, Text val, int numberOfPartitions) {
		return Math.abs(pair.userId.hashCode() % numberOfPartitions);
	}
}
