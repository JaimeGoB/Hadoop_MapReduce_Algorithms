package sortingfriendsbyage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator{

	//Setting up super class
    public FullKeyComparator() {
        super(UserFriendAgePair.class,true);
    }

    //Overriding compare method
    public int compare(WritableComparable obj1, WritableComparable obj2) {
    	
		UserFriendAgePair k1 = (UserFriendAgePair)obj1;
		UserFriendAgePair k2 = (UserFriendAgePair)obj2;

		int result = k1.userId.compareTo(k2.userId);
		
		if (result == 0) 
			return k1.friends_age.compareTo(k2.friends_age);
		else 
			return result;
    }
}
