package com.highill.practice.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperMain {

	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		String zookeeperUrl = "192.168.1.100:2181";
		int timeout = 3000;
		try
		{
			Watcher watcher = new Watcher()
			{

				@Override
				public void process(WatchedEvent event)
				{
					System.out.println("----- ----- event:  path=" + event.getPath() + ", typeValue=" + event.getType().getIntValue()
					        + ", stateValue=" + event.getState().getIntValue());

				}

			};
			ZooKeeper zookeeper = new ZooKeeper(zookeeperUrl, timeout, watcher);

			// creat base path
			String basePath = "/test_java";
			Stat basePathStat = zookeeper.exists(basePath, false);
			if (basePathStat != null)
			{
				System.out.println("----- exist path: " + basePath);
			} else
			{
				zookeeper.create(basePath, "test_java".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println("----- create path:" + basePath);
			}

			// create child path
			String childPath1 = basePath + "/data1";
			String childPath2 = basePath + "/data2";

			if (zookeeper.exists(childPath1, false) != null)
			{
				zookeeper.delete(childPath1, -1);
			}
			if (zookeeper.exists(childPath2, false) != null)
			{
				zookeeper.delete(childPath2, -1);
			}

			String child1Result = zookeeper.create(childPath1, "child_data1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("----- child1 create result: " + child1Result);
			String child2Result = zookeeper.create(childPath2, "child_data2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("----- child2 create result: " + child2Result);

			byte[] child1Bytes = zookeeper.getData(childPath1, false, zookeeper.exists(childPath1, false));
			System.out.println("----- child1Bytes: " + new String(child1Bytes));

			Stat child1Stat = zookeeper.setData(childPath1, "newChildData1".getBytes(), -1);
			byte[] child1NewBytes = zookeeper.getData(childPath1, false, child1Stat);
			System.out.println("----- child1NewBytes: " + new String(child1NewBytes));

			zookeeper.delete(childPath1, -1);
			zookeeper.delete(childPath2, -1);
			zookeeper.delete(basePath, -1);

			zookeeper.close();
		} catch (IOException | InterruptedException | KeeperException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
