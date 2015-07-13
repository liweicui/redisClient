package redis;





import org.springframework.beans.factory.InitializingBean;

import java.util.*;

/**
 * @ClassName: SharedJedisFactory
 * redis 分片技术工厂
 * @author jimmy.deng
 * @date 2014年5月15日 上午10:37:04 
 */

public class SharedJedisFactory implements InitializingBean {

	private String ips;

	private int maxActive;

	private int maxIdle;

	private int maxWait;

	public void setIps(String ips) {
		this.ips = ips;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	private boolean testOnBorrow;

	private Vector<RedisManager> redisManagers = new Vector<RedisManager>();

	@Override
	public void afterPropertiesSet() throws Exception {
		
		String[] arrayIp = ips.split("\\|");
		if (arrayIp == null)
			return;
		for (int i = 0; i < arrayIp.length; i++) {
			RedisManager r = RedisManager.getInstance(arrayIp[i], maxActive, maxIdle, maxWait, testOnBorrow);
			redisManagers.add(r);
		}
	}

	public Vector<RedisManager> getJedisPools() {
		return redisManagers;
	}

}
