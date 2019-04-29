package shizhan.zk.myzk;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

/**
 * 简单的zookeeper测试，需要使用CountDownLatch，否则会出现连接错误
 * @author PC
 *
 */
public class SimpleZkClient implements Watcher{
	
	private static final String connectString = "192.168.20.18:2181,192.168.20.20:2181,192.168.20.21:2181";
	private static final int sessionTimeout = 2000;
	ZooKeeper zkClient = null;
	private CountDownLatch countDownLatch = new CountDownLatch(1);
	
	
	@Before
	public void init() throws Exception{
		System.out.println("start connecting");
		zkClient = new ZooKeeper(connectString, sessionTimeout, this);
		countDownLatch.await();
	}
	
	public void testCreate() throws KeeperException, InterruptedException{
		/**
		 * 数据的增删改查
		 */
		//参数1：要创建节点的路径，参数2：节点数据，参数3：节点的权限，参数4：节点的类型
		String nodeCreated = zkClient.create("/myeclipse", "hellozk".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
	}
	
	
	/**
	 * 获取子节点
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void getChildren() throws KeeperException, InterruptedException{
		
		List<String> children = zkClient.getChildren("/", true);
		for(String child : children){
			System.out.println(child);
		}
		
		Thread.sleep(Long.MAX_VALUE);
	}


	/**
	 * 判断znode是否存在
	 * @throws Exception
	 */
	@Test
	public void testExist() throws Exception{
		Stat exists = zkClient.exists("/eclipse", false);
		System.out.println(exists == null ? "not exist" : "exist");
	}
	
	
	
	/**
	 * 获取节点数据
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * 
	 */
	@Test
	public void getData() throws KeeperException, InterruptedException {
		byte[] data = zkClient.getData("/app1", false, null);
		System.out.println(new String(data));
	}

	/**
	 * 删除节点数据
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * 
	 */
	@Test
	public void deleteZnode() throws KeeperException, InterruptedException {

		//参数2：指定删除的版本，-1表示所有版本
		zkClient.delete("/test", -1);
	}
	
	
	/**
	 * 获取节点数据
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * 
	 */
	@Test
	public void SetData() throws KeeperException, InterruptedException {

		zkClient.setData("/app1", "Hi, girl".getBytes(), -1);
	}
	
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println( "收到事件通知：" + event.getState() +"___" + event.getType() + "----" + event.getPath());
		if ( KeeperState.SyncConnected == event.getState() ) {
			countDownLatch.countDown();
		}

		try {
			zkClient.getChildren("/", true);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

}
