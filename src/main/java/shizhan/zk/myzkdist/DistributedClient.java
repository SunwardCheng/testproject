package shizhan.zk.myzkdist;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.client.ZooKeeperSaslClient.ServerSaslResponseCallback;


/**
 * zookeeper分布式客户端
 * @author PC
 *
 */
public class DistributedClient implements Watcher{
	
	private static final String connectString = "192.168.20.18:2181,192.168.20.20:2181,192.168.20.21:2181";
	private static final int sessionTimeout = 2000;
	ZooKeeper zkClient = null;
	private CountDownLatch countDownLatch = new CountDownLatch(1);
	private static final String parentNode = "/servers";
	
	private volatile List<String> serverList;
	
	/**
	 * 创建到zk的客户端连接
	 * @throws Exception
	 */
	public void getConnect() throws Exception{
		System.out.println("start connecting");
		zkClient = new ZooKeeper(connectString, sessionTimeout, this);
		countDownLatch.await();
	}
	
	public static void main(String[] args) throws Exception {
		
		//获取zk连接
		DistributedClient client = new DistributedClient();
		client.getConnect();
		//获取servers的子节点信息（并监听），从中获取服务器信息列表
		client.getServerList();
		//业务线程启动
		client.handleBussiness();
	}

	/**
	 * 获取服务器信息列表
	 * @throws Exception
	 */
	public void getServerList() throws Exception{
	
		//获取服务器子节点信息，并对父节点进行监听
		List<String> children = zkClient.getChildren(parentNode, true);
		//创建一个局部的list来存服务器信息
		List<String> servers = new ArrayList<String>();
		
		for (String child : children) {
			//child 只是子节点的节点名
			byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
			servers.add(new String(data));
		}
		//把servers赋值给成员变量，供各线程使用
		serverList = servers;
		
		//打印服务器列表
		System.out.println(serverList);
		
	}
	
	/**
	 * 业务功能
	 * @throws InterruptedException 
	 */
	public void handleBussiness() throws InterruptedException{
		System.out.println("client start working......");
		Thread.sleep(Long.MAX_VALUE);
	}
	@Override
	public void process(WatchedEvent event) {
		System.out.println( "收到事件通知：" + event.getState() +"___" + event.getType() + "----" + event.getPath());
		if ( KeeperState.SyncConnected == event.getState() ) {
			countDownLatch.countDown();
		}
		try {
			//重新更新服务器列表，并且注册了监听
			getServerList();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
