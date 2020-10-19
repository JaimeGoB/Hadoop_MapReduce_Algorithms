package sortingfriendsbyage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator{

	//Setting up super class
    public FullKeyComparator() {
        super(UserFriendAgePair.class,true);
    }

    //Overriding compare method
    /*
     * This class is in charge of sorting by the full key:
     * The has userId and friends_age both IntWritable,
     * I will sort based on both.
     * */
    public int compare(WritableComparable obj1, WritableComparable obj2) {
    	
		UserFriendAgePair k1 = (UserFriendAgePair)obj1;
		UserFriendAgePair k2 = (UserFriendAgePair)obj2;

		int result = Integer.compare(k1.userId, k2.userId);
		
		if (result == 0) 
			return Integer.compare(k1.friends_age, k2.friends_age);
		else 
			return result;
    }
}
