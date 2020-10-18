package sortingfriendsbyage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyComparator extends WritableComparator{

	//Setting up super class
    public NaturalKeyComparator() {
        super(UserFriendAgePair.class,true);
    }

    //Overriding compare method
    public int compare(WritableComparable a, WritableComparable b) {
    	
    	
		UserFriendAgePair k1 = (UserFriendAgePair)a;
		UserFriendAgePair k2 = (UserFriendAgePair)b;

		return k1.userId.compareTo(k2.userId);
    }
}