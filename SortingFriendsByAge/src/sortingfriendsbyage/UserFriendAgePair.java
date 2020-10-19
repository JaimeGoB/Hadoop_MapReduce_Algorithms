package sortingfriendsbyage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class UserFriendAgePair implements Writable,WritableComparable<UserFriendAgePair>{
    
	public int userId;
    public int friends_age;

    public UserFriendAgePair() {}
    
    public UserFriendAgePair(int userid, int f_age) {
    	this.userId = userid;
    	this.friends_age = f_age;
    }
    
    @Override
    public int compareTo(UserFriendAgePair other) {
    	
		int result = Integer.compare(this.userId, other.userId);
		
		if (result == 0) 
			return Integer.compare(this.friends_age, other.friends_age);
		else 
			return result;
    }
   
	@Override
	public void readFields(DataInput in) throws IOException {		
		userId = in.readInt();
		friends_age = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(userId);
		out.writeInt(friends_age);
	}
}