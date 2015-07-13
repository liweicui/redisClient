package redis;





import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;


public class SharedJedisTemplate implements InitializingBean {
	@Autowired
	SharedJedisFactory sharedJedisFactory;

	public void setSharedJedisFactory(SharedJedisFactory sharedJedisFactory) {
		this.sharedJedisFactory = sharedJedisFactory;
	}

	int poolSize;

	private RedisManager getJedis(String key) {
		int hashval = Math.abs(key.hashCode());
		int node = hashval % poolSize;
		RedisManager jedis = sharedJedisFactory.getJedisPools().get(node);
		return jedis;
	}

	public void put(String key, String value) {
		getJedis(key).put(key, value);
	}

	public void put(String key, String value, int timeout) {
		getJedis(key).put(key, value, timeout);
	}

	public void put(String key, byte[] object) {
		getJedis(key).put(key, object);
	}

	public void put(String key, byte[] object, int timeout) {
		getJedis(key).put(key, object, timeout);
	}

	public String get(String key) {
		return getJedis(key).get(key);
	}

	public byte[] getByte(String key) {
		return getJedis(key).getByte(key);
	}

	public boolean exists(String key) {
		return getJedis(key).exists(key);
	}

	public boolean existsByte(String key) {
		return getJedis(key).existsByte(key);
	}

	// *********************以下针对list的push,pop操作********************//

	public void push(String listName, String object) {
		push(listName, object.getBytes());
	}

	public void push(String listName, byte[] objects) {
		getJedis(listName).push(listName, objects);
	}

	public String pop(String listName) {
		return getJedis(listName).pop(listName);
	}

	public void hashPut(String hashName, String key, String value) {
		getJedis(hashName).hashPut(hashName, key, value);
	}

	public void hashLength(String hashName) {
		getJedis(hashName).hashLength(hashName);
	}

	public String hashGet(String hashName, String key) {
		return getJedis(hashName).hashGet(hashName, key);
	}

	public Map<String, String> hashGetAll(String hashName) {
		return getJedis(hashName).hashGetAll(hashName);
	}

	public void hashPut(String hashName, String key, byte[] value) {
		getJedis(hashName).hashPut(hashName, key, value);
	}

	public void hashByteLength(String hashName) {
		getJedis(hashName).hashByteLength(hashName);
	}

	public byte[] hashByteGet(String hashName, String key) {
		return getJedis(hashName).hashByteGet(hashName, key);
	}

	public void del(String key) {
		getJedis(key).flush(key);
	}
	
	
	
	//////////Sort-Set 操作
	
	public Long zcard(String key) {
		return getJedis(key).zcard(key);
	}

	
	 public void zdel(String key,String uId){
		 getJedis(key).zdel(key, uId);
	 }
	    
	 public void zadd(String key,Double score,String uId ){
	    	 getJedis(key).zadd(key, score, uId);
	 }
	   
	 public Map<String,String> zminScoreInfo(String key){
		Set<String> minSet =  getJedis(key).zrange(key, 0, 0);
		Map<String,String> returnMap = new HashMap<String, String>();
		if(null==minSet||minSet.size()==0){
			returnMap.put("uId", "");
			returnMap.put("score", "0");
			return returnMap;
		}
		
		Iterator<String>  ite =   minSet.iterator();
		String uId = "";
		while(ite.hasNext()){
		   uId = ite.next();
		   break;
		}
		
		Double score = getJedis(key).zscore(key, uId);
		returnMap.put("uId", uId);
		returnMap.put("score", String.valueOf(score));
		return returnMap;
	 }
	 
	 
	 
	 public void zmuiltAdd(String key,Map<String,Double> map){
		 getJedis(key).zmuiltAdd(key, map);
	 }
	 
	 
	 
	public Long  zrevrank(String key,String uId){
	    	
	    	return   getJedis(key).zrevrank(key, uId);
	    	
	}
	
	
	public Set<String> zrevrange(String key,Long begin,Long end){
		return   getJedis(key).zrevrage(key, begin, end);
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		poolSize = this.sharedJedisFactory.getJedisPools().size();

	}

}
