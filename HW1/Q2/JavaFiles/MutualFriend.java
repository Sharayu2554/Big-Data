package Q2TopMutualFriend;

public class MutualFriend  implements Comparable<MutualFriend>{
	
	private Integer count;
	private Integer user1;
	private Integer user2;
	private String friends;
	
	public MutualFriend() {
		super();
	}
	
	public MutualFriend(Integer count, Integer user1, Integer user2, String friends) {
		super();
		set(count, user1, user2, friends);
	}

	public void set(Integer count, Integer user1, Integer user2, String friends) {
		this.count = count;
		this.user1 = user1;
		this.user2 = user2;
		this.friends = friends;
	}

	
	@Override
	public int compareTo(MutualFriend o) {
		if(count > o.getCount())
		{  
	        return 1;  
	    }
		else if(count < o.getCount())
		{  
	        return -1;  
	    }
		else{  
			return 0;
	    }
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public Integer getUser1() {
		return user1;
	}

	public void setUser1(Integer user1) {
		this.user1 = user1;
	}

	public Integer getUser2() {
		return user2;
	}

	public void setUser2(Integer user2) {
		this.user2 = user2;
	}

	public String getFriends() {
		return friends;
	}

	public void setFriends(String friends) {
		this.friends = friends;
	}

	@Override
	public String toString() {
		return "MutualFriend [count=" + count + ", user1=" + user1 + ", user2=" + user2 + ", friends=" + friends + "]";
	}

}
