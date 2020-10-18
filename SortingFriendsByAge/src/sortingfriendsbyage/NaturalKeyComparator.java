package sortingfriendsbyage;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyComparator extends WritableComparator{

	//Setting up super class
    public NaturalKeyComparator() {
        super(UserFriendAgePair.class,true);
    }

    //Overriding compare method
    /*
     * We will use this to group by the same natural key,
     * in our case the natural key is user.
     * Thus we want to group all objects with the same userID
     * (Note this is userId of person NOT OF FRIENDS)
     * */
    public int compare(WritableComparable a, WritableComparable b) {
    	
    	
		UserFriendAgePair k1 = (UserFriendAgePair)a;
		UserFriendAgePair k2 = (UserFriendAgePair)b;

		return k1.userId.compareTo(k2.userId);
    }
}