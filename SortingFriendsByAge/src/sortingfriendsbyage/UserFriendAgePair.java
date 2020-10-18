package sortingfriendsbyage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class UserFriendAgePair implements Writable,WritableComparable<UserFriendAgePair>{
    
	public IntWritable userId = new IntWritable();
    public IntWritable friends_age = new IntWritable();

    public UserFriendAgePair() {}
    
    public UserFriendAgePair(IntWritable userid, IntWritable f_age) {
    	this.userId = userid;
    	this.friends_age = f_age;
    }
    
    @Override
    public int compareTo(UserFriendAgePair o) {
    	
		int result = this.userId.compareTo(o.userId);
		
		if (result == 0) 
			return this.friends_age.compareTo(o.friends_age);
		else 
			return result;
    }
   
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
}