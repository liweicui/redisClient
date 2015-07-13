package redis;





import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisManager{  
	 
	
	RedisManagerProxy proxy;
	
	/**
	 * 杩炴帴ip鏍煎紡锛岄粯璁ょ涓�涓负涓绘湇鍔″櫒,鍚庨潰鐨勪负浠庢湇鍔″櫒,鑻ュ彧璁剧疆涓�涓紝鍒欎负涓绘湇鍔″櫒
	 */
	public static final String DEFAULTIPFORMAT = "127.0.0.1:6379,127.0.0.1:6380"; 
	
	public static ConcurrentHashMap<String, RedisManager> redisMap= new ConcurrentHashMap<String,RedisManager>();
	
	boolean enableReadWriteSeparation;
	
	ThreadLocal<OP> cop = new ThreadLocal<OP>();
	
	final static Logger logger = LoggerFactory.getLogger(RedisManager.class);
	
	
	  
	public static RedisManager getInstance(String ips,int maxActive,int maxIdle,int maxWait,boolean testOnBorrow){
		RedisManager instance = redisMap.get(ips);
		if(instance==null){
			RedisManagerProxy proxy = new RedisManagerProxy(ips,maxActive,maxIdle,maxWait,testOnBorrow);
			instance = (RedisManager)proxy.getProxy(new RedisManager(proxy));
			redisMap.put(ips, instance);
	   }
	   return instance;   
	}   
  
	
    public RedisManager() {
		super();
	}


	private RedisManager(RedisManagerProxy proxy){
    	this.proxy = proxy;
    }
    
    private void initOP(OP op){
    	if(enableReadWriteSeparation){
    		cop.set(op);
    	}else{
    		cop.set(OP.READORWRITE);
    	}
    }
    
    public void enableReadWriteSeparation(boolean enable){
    	enableReadWriteSeparation = enable;
    }
    
  //************************浠ヤ笅閽堝鍗曞瓧绗︿覆鐨勬搷浣�***********************//  
 
    public void put(String key, String value) {
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().set(key, value);
    }   
  
    public void put(String key, String value, int timeout) {
    	initOP(OP.WRITE);
    	Jedis jedis = proxy.getCurrJedis();
    	jedis.set(key, value);
    	jedis.expire(key, timeout);
    }
    
    public void put(String key, byte[] object) {
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().set(key.getBytes(), object);
    }   
  
    public void put(String key, byte[] object, int timeout) {
    	initOP(OP.WRITE);
    	Jedis jedis = proxy.getCurrJedis();
    	jedis.set(key.getBytes(), object);
    	jedis.expire(key.getBytes(), timeout);
    }
    
    public void flush(String... keys) {
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().del(keys); 
    }
    
    public void flushByte(String... keys) {
    	initOP(OP.WRITE);
    	byte[][] b = new byte[keys.length][];
    	for (int i=0;i<b.length;i++){
    		b[i] = keys[i].getBytes();
    	}
    	proxy.getCurrJedis().del(b); 
    }
    
    public String get(String key) {
    	initOP(OP.READ);
        return  proxy.getCurrJedis().get(key);
    }
    
    public byte[] getByte(String key) {
    	initOP(OP.READ);
        return  proxy.getCurrJedis().get(key.getBytes());
    }
    
    public boolean exists(String key){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().exists(key);	
    } 
    
    public boolean existsByte(String key){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().exists(key.getBytes());	
    } 
    
    //*********************浠ヤ笅閽堝list鐨刾ush,pop鎿嶄綔********************//
    
    public void push(String listName,String  object){
    	push(listName,object.getBytes());
    }
    
    public void push(String listName,byte[] objects){
    	initOP(OP.WRITE);
		proxy.getCurrJedis().rpush(listName.getBytes(), objects);
    }

	interface MessageListener{
		public abstract void beforeMessage();
		public abstract void onMessage(byte[] msg);
	}

	class BlockingMessageListener implements MessageListener{
		Object popLock = new Object();
		byte[] msg;
		volatile boolean needwait = true;
		@Override
		public void onMessage(byte[] msg) {
			// TODO Auto-generated method stub
			this.msg = msg;
			synchronized(popLock){
			  popLock.notifyAll();
			  needwait = false;
		   	} 
		}
        public byte[] getMsg(){
        	return msg;
        }
		@Override
		public void beforeMessage() {
			// TODO Auto-generated method stub
		   	synchronized(popLock){
		   		try {
		   		    
					if (needwait)popLock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		   	} 
		}
		
	}
	class BlockingPoPThread extends Thread{
		int timeout;
		String listName;
		Jedis curr = null;
		LinkedBlockingQueue<MessageListener> listeners = new  LinkedBlockingQueue<MessageListener>();
		public void addListener(MessageListener ml){
			listeners.add(ml);
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			//涓嶅彲鏂紑杩炴帴
	    	while(true){
	    		byte[] ret = null;
	    		MessageListener first = null;
	    		try{
					first = listeners.take();//鍙栧嚭涓�涓秷璐硅��,娌℃湁鍒欑瓑寰�
					curr = proxy.createJedis();//鏂板缓涓�涓柊鐨勶紝涓嶈兘鍙栧綋鍓嶇嚎绋嬪��
		    		ret = curr.blpop(timeout, listName.getBytes("utf-8")).get(1);//鍙栧嚭涓�涓秷鎭紝娌℃湁鍒欑瓑寰�
	        	}catch(Exception e){
	        		logger.error(e.getMessage());
	        	}finally{	
					if (first != null )first.onMessage(ret);
					proxy.gcJedis(curr);
	        	}
	    	} 	
		}			
		public void interrupt(){
			curr.disconnect();
		}
		public BlockingPoPThread(int timeout, String listName) {
			super();
			this.timeout = timeout;
			this.listName = listName;
		}
		
	}
	private ConcurrentHashMap<String,BlockingPoPThread> popThreads = new ConcurrentHashMap<String,BlockingPoPThread>();
	
    public String pop(String listName){
    	return pop(listName,false);
    }
    
    public long llen(String listName){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().llen(listName);
    }
    
    public String pop(String listName,boolean blockingWhenEmpty){
    	//涓�鐩撮樆濉炵姸鎬�
    	return pop(listName,blockingWhenEmpty,0);
    }
    
	public String pop(final String listName,final boolean blockingWhenEmpty,final int timeout){
    	return new String(popByte(listName,blockingWhenEmpty,timeout));
    }
    
    public byte[] popByte(String listName){
    	initOP(OP.WRITE);
    	return proxy.getCurrJedis().lpop(listName.getBytes());
    }
    
    public byte[] popByte(String listName,boolean blockingWhenEmpty){
    	return popByte(listName,blockingWhenEmpty,0);
    }
    public byte[] popByte(String listName,boolean blockingWhenEmpty,int timeout){
    	initOP(OP.WRITE);
    	if (blockingWhenEmpty){
    		popThreads.putIfAbsent(listName+timeout, new BlockingPoPThread(timeout,listName));
        	final BlockingPoPThread bpp = popThreads.get(listName+timeout);
        	BlockingMessageListener bml = new BlockingMessageListener();
        	bpp.addListener(bml);
        	if (!bpp.isAlive()){
        		bpp.setDaemon(true);
        		bpp.start();
            	proxy.registerChangeMaster(new ChangeMasterListener(){
        			@Override
        			public void changeMaster(String ip, int port) {
        				// TODO Auto-generated method stub
        				bpp.interrupt();
        			}
            	});
        	}
        	bml.beforeMessage();
            byte[] ret = bml.getMsg();
    		return ret;
    	}else{
    		return proxy.getCurrJedis().lpop(listName.getBytes());
    	}
    }
    
    
    //***********************浠ヤ笅閽堝hash琛ㄦ搷浣�*********************//
    
    public void hashPut(String hashName,String key,String value){
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().hset(hashName, key, value);
    }
    
    public void hashLength(String hashName){
    	initOP(OP.READ);
    	proxy.getCurrJedis().hlen(hashName);
    }
    
    public String hashGet(String hashName,String key){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().hget(hashName, key);
    }
    public Map<String,String> hashGetAll(String hashName){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().hgetAll(hashName);
    }
    
    public void hashPut(String hashName,String key,byte[] value){
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().hset(hashName.getBytes(), key.getBytes(), value);
    }
    
    public void hashByteLength(String hashName){
    	initOP(OP.READ);
    	proxy.getCurrJedis().hlen(hashName.getBytes());
    }
   
    public byte[] hashByteGet(String hashName,String key){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().hget(hashName.getBytes(), key.getBytes());
    }
    
    
    
    //***************Sort-Set鎿嶄綔**********************
    
    public Long zcard(String key){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().zcard(key);
    }
    
    public void zdel(String key,String uId){
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().zrem(key, uId);
    }
    
    public void zadd(String key,Double score,String uId ){
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().zadd(key, score, uId);
    }
    
    public Set<String> zrange(String key,long start,long end){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().zrange(key, start, end);
    }
    
    public Double zscore(String key,String uId){
    	initOP(OP.READ);
    	return proxy.getCurrJedis().zscore(key, uId);
    }
    
    
    
    public void zmuiltAdd(String key,Map<String ,Double> scoreMembers){
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().zadd(key, scoreMembers);
    	
    }
    
    
    public Long  zrevrank(String key,String uId){
    	
    	initOP(OP.READ);
    	return  proxy.getCurrJedis().zrevrank(key, uId);
    	
    }
    
    public Set<String>  zrevrage(String key,Long start,Long end){
    	initOP(OP.READ);
    	return  proxy.getCurrJedis().zrevrange(key, start, end);
    }
    
    
    //**********************鍙戝竷璁㈤槄channel**************************//

	public void subscribe(final String interestingName,final SubscribeAdapter callback){
		initOP(OP.READ);
    	class subscribeThread extends Thread{

    		Jedis curr = null;
    		
			@Override
			public void run() {
				// TODO Auto-generated method stub
				//涓嶅彲鏂紑杩炴帴
		    	while(true){
		    		try{
		    			curr = proxy.createJedis();//鏂板缓涓�涓柊鐨勶紝涓嶈兘get浜�
		    			curr.subscribe(callback, interestingName);
		        	}catch(Exception e){
		        		logger.error(e.getMessage());
		        		try {
							Thread.sleep(2000);
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							
						}
		        	}finally{
		        		proxy.gcJedis(curr);
		        	}
		    	} 	
			}
			
			public void interrupt(){
				curr.disconnect();
			}
    		
    	}
    	final Thread subscribe = new subscribeThread();
    	//娉ㄥ唽master鍙樺寲浜嬩欢
    	proxy.registerChangeMaster(new ChangeMasterListener(){

			@Override
			public void changeMaster(String ip, int port) {
				// TODO Auto-generated method stub
				subscribe.interrupt();
			}
    		
    	});
    	subscribe.setDaemon(true);
    	subscribe.start();
    	try {
			subscribe.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void publish(String channelName,String message){
    	initOP(OP.WRITE);
    	proxy.getCurrJedis().publish(channelName, message);

    }
    
    //*******************************鍏跺畠鎿嶄綔锛岄渶鐢ㄥ啀鎵╁睍***********************************************//
    
    
    
    
	public static void main(String[] arg){
        final RedisManager rm = RedisManager.getInstance("192.168.6.154:6379,192.168.6.154:6380",50,5,5000,true);
        
        rm.put("zhang", "test");
     	System.out.println(rm.get("zhang"));
     	/*for (int i=0;i<50;i++){
     		System.out.println(rm.get("zhang"));
     		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
     	}
    	for (int i=0;i<10;i++){
     		rm.push("testlist", String.valueOf(i));
     	}
     	
    	for (int i=0;i<10;i++){
     		System.out.println(rm.pop("testlist",true));
     	}  
    	new Thread(){
			@Override
			public void run() {
				// TODO Auto-generated method stub
				 rm.subscribe("order.StatusChangeTopic", new SubscribeAdapter(){
					@Override
					public void onMessage(String channel, String message) {
						// TODO Auto-generated method stub
						System.out.println("receive:"+message);
					}
				 });
			}
    	}.start();
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	rm.publish("order.StatusChangeTopic", "hello.....");*/
     }

	public static class SubscribeAdapter extends JedisPubSub{

		@Override
		public void onMessage(String channel, String message) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
}

//鎿嶄綔绫诲瀷
enum OP {READORWRITE,READ,WRITE};

class RedisManagerProxy implements MethodInterceptor{

	final static Logger logger = LoggerFactory.getLogger(RedisManager.class);
	HAJedisPool  jedisPool;
    private static final ThreadLocal<Jedis> currJedis = new ThreadLocal<Jedis>();
    List<ChangeMasterListener> obserers = new ArrayList<ChangeMasterListener>();
    
	public RedisManagerProxy(String ips,int maxActive,int maxIdle,int maxWait,boolean testOnBorrow) {
		Config config = new Config();
    	config.maxActive = maxActive; //50;
    	config.maxIdle = maxIdle; //5;
    	config.maxWait = maxWait; // 5000;
    	config.testOnBorrow = testOnBorrow;//true;
    	List<HAJedisInfo> list = new ArrayList<HAJedisInfo>();
    	try{
    	String[] ip = ips.split(",");
    	for (int i=0;i<ip.length;i++){
    		String[] ipinfo = ip[i].split(":");
    		if (ipinfo.length == 2){
    			HAJedisInfo jedisInfo = new HAJedisInfo(ipinfo[0],Integer.valueOf(ipinfo[1]));
        		if (i == 0) jedisInfo.setIsmaster(true);
    			list.add(jedisInfo);
    		}
    		
    	}}catch(Exception e){
    		logger.error("ip鏍煎紡涓嶅锛岀ず渚�:"+RedisManager.DEFAULTIPFORMAT);
    		System.exit(-1);
    	}
    	if (list.size()==0){
	    	logger.error("ip鏍煎紡涓嶅锛岀ず渚�:"+RedisManager.DEFAULTIPFORMAT);
	    	System.exit(-1);
	    }
    	jedisPool  = new HAJedisPool(config,list);
	}
	public void registerChangeMaster(ChangeMasterListener lis){
		this.obserers.add(lis);
	}
	public void gcJedis(Jedis jedis) {
		jedisPool.returnResource(jedis);
	}
	public Jedis createJedis(){
		return jedisPool.getResource();
	}
	public Jedis getCurrJedis(){
		return currJedis.get();
	}
	RedisManager target;
	public Object getProxy(Object target){
		 this.target = (RedisManager) target;
		 Enhancer enhancer = new Enhancer();  
		 enhancer.setSuperclass(target.getClass());  
		 enhancer.setCallback(this); 
	     return enhancer.create(); 	
	}
	
	@Override
	public Object intercept(Object arg0, Method arg1, Object[] arg2,
			MethodProxy arg3) throws Throwable {
		// TODO Auto-generated method stub
		try{
		    if (target.cop.get()==OP.READ){
		    	currJedis.set(jedisPool.getSlaveResource());
		   // 	logger.info("now get read jedis from:"+currJedis.get());
		    }else{
		    	currJedis.set(jedisPool.getResource());
		    //	logger.info("now get jedis from:"+currJedis.get());
		    }
			Object oj = arg3.invoke(target, arg2);
			return oj;
		}catch(Throwable e){
			if (e instanceof InvocationTargetException){
				e = ((InvocationTargetException) e).getTargetException();
			}
			logger.error(e.getMessage(),e);
		}finally{
			if (target.cop.get()==OP.READ){
				jedisPool.returnSlaveResource(currJedis.get());
		    }else{
				jedisPool.returnResource(currJedis.get());
		    }

			currJedis.remove();
		}
		return null;
	}
	
	//鏀寔鍙屾満涓讳粠澶嶅埗鐨勫け璐ヨ嚜鍔ㄩ�夋嫨鍜岃嚜鍔╩aster鎭㈠鐨勮繛鎺ユ睜
	private  class HAJedisPool{
		
		//璇诲啓鍒嗙鏀寔
		private final GenericObjectPool slavePool;
		
		private final GenericObjectPool masterPool;
		
		 public HAJedisPool(Config config,String ip,int port,int timeout) {
			 List<HAJedisInfo> HAJedisInfo = new ArrayList<HAJedisInfo>();
			 HAJedisInfo ha = new HAJedisInfo(ip,port,timeout);
			 ha.setIsmaster(true);
			 HAJedisInfo.add(ha);
			 HAJedisFactory hafc = new HAJedisFactory(HAJedisInfo);
			 this.masterPool = new GenericObjectPool( hafc, config);
			 this.slavePool = new GenericObjectPool(new HAJedisSlaveFactory(HAJedisInfo,hafc) , config);
			
		}
		 public HAJedisPool(Config config,List<HAJedisInfo> HAJedisInfo) {
			 HAJedisFactory hafc = new HAJedisFactory(HAJedisInfo);
			 this.masterPool = new GenericObjectPool( hafc, config);
			 this.slavePool = new GenericObjectPool(new HAJedisSlaveFactory(HAJedisInfo,hafc) , config);
		}
		
		 @SuppressWarnings("unchecked")
		 public Jedis getResource() {
		        try {
		            return (Jedis) masterPool.borrowObject();
		        } catch (Exception e) {
		            throw new JedisConnectionException(
		                    "鑾峰彇jedis杩炴帴澶辫触", e);
		        }
		 }
		 
		 @SuppressWarnings("unchecked")
		 public Jedis getSlaveResource() {
		        try {
		            return (Jedis) slavePool.borrowObject();
		        } catch (Exception e) {
		            throw new JedisConnectionException(
		                    "鑾峰彇jedis杩炴帴澶辫触", e);
		        }
		 }
		        
		public void returnResource(final Object resource) {
		        try {
		        	masterPool.returnObject(resource);
		        } catch (Exception e) {
		            throw new JedisException(
		            		  "鍥炴敹jedis杩炴帴澶辫触", e);
		        }
		    }
		
		public void returnSlaveResource(final Object resource) {
	        try {
	        	slavePool.returnObject(resource);
	        } catch (Exception e) {
	            throw new JedisException(
	            		  "鍥炴敹jedis杩炴帴澶辫触", e);
	        }
	    }
		
		private void destoryMasterAll(){
			if (masterPool != null){
				masterPool.clear();
			}	
		}
		private void destorySlaveAll(){
			if (slavePool != null){
				slavePool.clear();
			}	
		}

        private class HAJedisSlaveFactory extends BasePoolableObjectFactory implements ChangeMasterListener{
            private HAJedisFactory masterFactory;
            private List<HAJedisInfo> HAJedisList;
            private HAJedis currentSlave;
            Logger logger = LoggerFactory.getLogger(RedisManager.class);
			public HAJedisSlaveFactory(List<HAJedisInfo> HAJedisInfo,
					HAJedisFactory masterFactory) {
				this.HAJedisList = HAJedisInfo;
				this.masterFactory = masterFactory;
				obserers.add(this);
			}
			private void switchIp(){
				destorySlaveAll();
				findBaseSlave();
				logger.info("switchToSlave:"+currentSlave);
			}
			@Override
			public Object makeObject() throws Exception {
				// TODO Auto-generated method stub
				if (currentSlave == null || !checkIsAlive(currentSlave)){
					switchIp();
				}
				HAJedis jedis = new HAJedis(currentSlave.getIp(),currentSlave.getPort(),currentSlave.getTimeout());
				return jedis;
			}
			@Override
		    public void destroyObject(final Object obj) {
				masterFactory.destroyObject(obj);
		    }
			@Override
			public boolean validateObject(final Object obj) {
				try {
	        		HAJedis jedis = (HAJedis) obj;
	                return (currentSlave.equals(jedis)
	                        )
	                		&&checkIsAlive(jedis);
	            } catch (Exception ex) {
	                return false;
	            }
		    }
			
			private boolean checkIsAlive(HAJedis jedis){
				return  masterFactory.checkIsAlive(jedis);
			}
			private boolean checkIsAlive(String ip,int port){
				return  masterFactory.checkIsAlive(ip,port);
			}
			private synchronized void findBaseSlave(){
				try{
	        	boolean finded = false;
	        	for (HAJedisInfo ha : HAJedisList){
	        		//绗竴娆￠�夋椂
	        		if (currentSlave == null){
	        			if (!ha.isIsmaster() 
	        					&& !masterFactory.currentMaster.equals(ha.getIp(), ha.getPort())
	        					&& checkIsAlive(ha.getIp(),ha.getPort())){
		        			currentSlave = new HAJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
		        			finded = true;
		        			break;
		        		}
	        		}
	        		//鍚庣户閫夋嫨鏃�
	        		if (!masterFactory.currentMaster.equals(ha.getIp(), ha.getPort())
	        				&& checkIsAlive(ha.getIp(),ha.getPort())){
	        			this.destroyObject(currentSlave);
	        			currentSlave = new HAJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
	        			finded = true;
	        			break;
	        		}
	        	}
	        	if (!finded) {
	        		//涓�娆℃湭鎵惧埌鍙敤鐨勫悗锛屽垯涓嶅啀灏濊瘯妫�娴嬩粠鏈嶅姟鍣ㄧ殑鐨勭姸鎬侊紝鐩存帴浣跨敤涓绘湇鍔″櫒
	        		logger.warn("鏈壘鍒板彲鐢ㄧ殑浠庢湇鍔″櫒,鍒囨崲鍒颁富鏈嶅姟鍣ㄦ湇鍔�!");
	        		currentSlave = masterFactory.currentMaster;
	        		logger.warn("鍒囨崲鍒扮殑涓绘湇鍔″櫒:"+currentSlave);
	        	}
				}catch(Exception e){
					logger.error(e.getMessage(),e);
				}
	        }
			@Override
			public void changeMaster(String ip, int port) {
				// TODO Auto-generated method stub
				switchIp();
			}
        }
		private  class HAJedisFactory extends BasePoolableObjectFactory{
		        private List<HAJedisInfo> HAJedisList;
		        private HAJedisInfo currentActiveHAJedis;
		        private volatile boolean isBaseMaster;
		        private HAJedis baseMaster;
		        private HAJedis currentMaster;
		        Logger logger = LoggerFactory.getLogger(RedisManager.class);
		       
		        public HAJedisFactory(List<HAJedisInfo> HAJedisInfo) { 
		            this.HAJedisList = HAJedisInfo;
		            findBaseMaster();   
		            Thread heart = new Thread(new HeartBeat());
		            heart.setDaemon(true);
		            heart.start();
		        }
		        
		        public synchronized Object makeObject() throws Exception {
		        	checkConnection();
		        	HAJedis jedis = new HAJedis(currentActiveHAJedis.getIp(),currentActiveHAJedis.getPort(),currentActiveHAJedis.getTimeout());
		        	return jedis;
		        }
		        
		        private synchronized void switchIp(){
		        	destoryMasterAll();
		        	findOneSavleAsMaster();
		        }
		        
		        private synchronized void findOneSavleAsMaster(){
		        	for (HAJedisInfo ha : HAJedisList){
		        		String currIP = currentActiveHAJedis.getIp()+":"+currentActiveHAJedis.getPort();
		        		String haIP = ha.getIp()+":"+ha.getPort();
		        		if (!ha.isIsmaster() 
		        				&&!currIP.equals(haIP)){
		        			HAJedis jedis = null;
		        			try{
		        			jedis = new HAJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
		        			if (checkIsAlive(jedis))
		        				{
		        				currentActiveHAJedis = ha;
		        				isBaseMaster = false;
		        				changeMasterTo(ha.getIp(),ha.getPort());
		        				break;
		        				}
		        			}finally{
		        				this.destroyObject(jedis);
		        			}
		        			
		        		}
		        	}
		        }
		        
		        private synchronized void findBaseMaster(){
		        	boolean finded = false;
		        	for (HAJedisInfo ha : HAJedisList){
		        		if (ha.isIsmaster()){
		        			baseMaster = new HAJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
		        			currentMaster = baseMaster;
		        			isBaseMaster = true;
		        			currentActiveHAJedis = ha;
		        			finded = true;
		        			break;
		        		}
		        	}
		        	if (!finded || !checkIsAlive(baseMaster)) {
		        		logger.warn("鏈缃富鏈嶅姟鍣ㄦ垨涓绘湇鍔″櫒褰撳墠涓嶅彲鐢�,閫夋嫨鍏跺畠slave浣滀负涓绘湇鍔″櫒!");
		        		switchIp();
		        	}
		        }
		        
		        //濮嬬粓淇濇寔褰撳墠娲诲姩鐨勬湇鍔′负涓绘湇鍔＄姸鎬�
		        private synchronized void changeMasterTo(String toip,int toport){
		        	destoryMasterAll();
		        	for (HAJedisInfo ha : HAJedisList){
		        		if (toip.equals(ha.getIp()) && (toport == ha.getPort())){
		        			if (!baseMaster.equals(currentMaster))destroyObject(currentMaster);
		        			this.currentMaster = new HAJedis(ha.getIp(),ha.getPort(),ha.getTimeout());
		        			if (baseMaster.getIp().equals(ha.getIp())
		        				&&baseMaster.getPort() == ha.getPort())
		        				{
		        				isBaseMaster = true;
		        				}else{
		        				isBaseMaster = false;	
		        				}
		        			currentActiveHAJedis = ha;
		        			
		        		}
		        	}
		        	HAJedis jedis = new HAJedis(toip,toport);
		        	jedis.slaveofNoOne();
		        	destroyObject(jedis);
		        	//鍏跺畠鎵�鏈夎妭鐐归兘璁剧疆浠庤妭鐐�
		        	Jedis other = null;
		        	for (HAJedisInfo ha : HAJedisList){
		        		try{
		        			if (!toip.equals(ha.getIp()) && (toport != ha.getPort())){
		        				other = new HAJedis(ha.getIp(),ha.getPort());
		        				other.slaveof(toip, toport);
		        			}
							
		        		}catch(Exception e){}finally{
		        			destroyObject(other);
		        		}
		        	}
		        	for (ChangeMasterListener cm : obserers){
		        		cm.changeMaster(currentActiveHAJedis.getIp(), currentActiveHAJedis.getPort());
		        	}
					logger.warn("switchToMaster-->"+currentActiveHAJedis.getIp()+":"+currentActiveHAJedis.getPort());
		        }
		        
		        private boolean checkIsAlive(Jedis jedis){
		        	try{
		        		return jedis.ping().equals("PONG");
		        	}catch(Exception e){
		        		logger.error(e.getMessage());
		        		try{
		        		jedis.disconnect();
		        		}catch(Exception e1){};
		        	}
		        	return false;
		        }
		        
		        private boolean checkIsAlive(String ip,int port){
		        	HAJedis jedis = null;
		        	try{
		        		jedis = new HAJedis(ip,port);
		        		return jedis.ping().equals("PONG");
		        	}catch(Exception e){
		        		logger.error(e.getMessage());
		        	}finally{
		        		this.destroyObject(jedis);
		        	}
		        	return false;
		        }
		        
		        public void destroyObject(final Object obj) {
		            if ((obj != null) && (obj instanceof Jedis)) {
		            	Jedis jedis = (Jedis) obj;
		            	 try {
		                   		try {
		                   			jedis.quit();
		                        } catch (Exception e) {

		                        }
		                        jedis.disconnect();
		                    } catch (Exception e) {

		                    }

		                }
		        }
		        //蹇呴』鏄富鑺傜偣涓旀槸娲诲姩鐘舵��
		        public boolean validateObject(final Object obj) {
		        	try {
		        		Jedis jedis = (Jedis) obj;
		                return (currentMaster.equals(jedis)
		                        )
		                		&&checkIsAlive(jedis);
		            } catch (Exception ex) {
		                return false;
		            }
		        }
		        
		        private void checkConnection(){
		        	try{
						if (isBaseMaster && !checkIsAlive(baseMaster)){
							logger.warn("褰撳墠鏄痓asemaster,蹇冭烦妫�娴嬪埌鍏朵笉鍙敤锛岄�夋嫨涓�涓猻lave鏇挎崲涓簃aster");
			        		switchIp();
			        	}
						//妫�娴嬪彲鐢ㄦ椂锛屽鏋滃綋鍓嶄笉鏄痬aster,鍒欐仮澶嶅埌master
						//瑙勫垯锛屾妸褰撳墠浠庣殑璁剧疆鎴愪富鐨勶紝鎭㈠鐨勪富鐨勮缃垚浠庣殑锛屼繚璇佹暟鎹竴鑷存��
						if (!isBaseMaster && checkIsAlive(baseMaster)){
							logger.warn("妫�娴媌asemaster鍙敤,鎭㈠鍒癰asemaster");
							String ip = baseMaster.getIp();
							int port = baseMaster.getPort();
							//鍚屾鏁版嵁
							logger.warn("鍚屾鏂版暟鎹埌master");
							changeMasterTo(ip,port);
							isBaseMaster = true;
						}
						//濡傛灉褰撳墠savle涓嶅彲鐢紝鍒欐崲鍙︿竴涓猻avle
						if (!isBaseMaster && !checkIsAlive(currentMaster)){
							logger.warn("褰撳墠currentmaster涓嶅彲鐢紝鎹㈠彟涓�涓猻lave涓篶urrentmaster");
							switchIp();
						}
						}catch(Exception e){
							logger.error(e.getMessage(),e);
						}
						
		        }

		        class HeartBeat implements Runnable{
		        	
		        	Logger logger = LoggerFactory.getLogger(RedisManager.class);
		        	//蹇冭烦妫�娴�
					@Override
					public void run() {
						// TODO Auto-generated method stub
					    while(true){
					    checkConnection();
						 try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					  }
					   
					}
		        }
		        
				}
	  }
}

interface ChangeMasterListener{
	public abstract void changeMaster(String ip,int port);
}
class HAJedis extends Jedis{

	public HAJedis(String host, int port, int timeout) {
		super(host, port, timeout);
		// TODO Auto-generated constructor stub
	}

	public HAJedis(String host, int port) {
		super(host, port);
		// TODO Auto-generated constructor stub
	}

	public HAJedis(String host) {
		super(host);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj == null) return false;
		Jedis jedis = (Jedis)obj;
		return this.getClient().getHost().equals(jedis.getClient().getHost())
		&& this.getClient().getPort()==jedis.getClient().getPort();
	}
	
	public boolean equals(String ip,int port) {
		// TODO Auto-generated method stub
		return this.getClient().getHost().equals(ip)
		&& this.getClient().getPort()==port;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.getClient().getHost()+":"+this.getClient().getPort();
	}
	
	public String getIp(){
		return this.getClient().getHost();
	}
	public int getPort(){
		return this.getClient().getPort();
	}
	public int getTimeout(){
		return this.getClient().getTimeout();
	}
	
}
class HAJedisInfo{
	private String ip;
	private int port;
	private int timeout;
	private boolean ismaster;
	
	public HAJedisInfo(String ip, int port, int timeout) {
		super();
		this.ip = ip;
		this.port = port;
		this.timeout = timeout;
	}
	public HAJedisInfo(String ip, int port) {
		super();
		this.ip = ip;
		this.port = port;
		this.timeout = 5000;
	}
	
	public boolean isIsmaster() {
		return ismaster;
	}

	public void setIsmaster(boolean ismaster) {
		this.ismaster = ismaster;
	}

	public boolean testisConnection(){
	 return true;	
	}
	
	public int getTimeout() {
		return timeout;
	}
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
}

