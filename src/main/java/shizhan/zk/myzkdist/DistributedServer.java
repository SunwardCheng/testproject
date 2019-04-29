package shizhan.zk.myzkdist;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;


/**
 * zookeeper分布式服务端
 * @author PC
 *
 */
public class DistributedServer implements Watcher{
	
	private static final String connectString = "192.168.20.18:2181,192.168.20.20:2181,192.168.20.21:2181";
	private static final int sessionTimeout = 2000;
	ZooKeeper zkClient = null;
	private CountDownLatch countDownLatch = new CountDownLatch(1);
	private static final String parentNode = "/servers";
	
	
	/**
	 * 创建到zk的客户端连接
	 * @throws Exception
	 */
	public void getConnect() throws Exception{
		System.out.println("start connecting");
		zkClient = new ZooKeeper(connectString, sessionTimeout, this);
		countDownLatch.await();
	}
	
	/**
	 * 向zk集群注册服务器信息
	 * @param hostName
	 * @throws Exception
	 */
	public void registerServer(String hostName) throws Exception{
		
		String create = zkClient.create(parentNode + "/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostName + " is online " + create);
	}
	
	/**
	 * 业务功能
	 * @throws InterruptedException 
	 */
	public void handleBussiness(String hostName) throws InterruptedException{
		System.out.println(hostName + "start working......");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		//获取zk连接
		DistributedServer server = new DistributedServer();
		server.getConnect();
		//利用zk连接注册服务器信息
		server.registerServer(args[0]);
		//启动业务功能
		server.handleBussiness(args[0]);
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
