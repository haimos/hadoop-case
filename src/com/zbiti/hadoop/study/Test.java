package com.zbiti.hadoop.study;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Test {
	public static class TreeMapKey implements Comparable<TreeMapKey> {
		private int count;
		private String cateId;

		public int getCount() {
			return count;
		}

		public String getCateId() {
			return cateId;
		}

		public TreeMapKey(int count, String cateId) {
			this.count = count;
			this.cateId = cateId;
		}

		public int compareTo(TreeMapKey o) {
			int firstCompare = -(this.count - o.count);
			if (firstCompare == 0) {
				return -this.cateId.compareTo(o.cateId);
			}
			return firstCompare;
		}

	}

	

	public static void main(String[] args) {
		 TreeMap<TreeMapKey, Integer> treeMap = new TreeMap<TreeMapKey, Integer>();
		 int k = 10;
		treeMap.put(new TreeMapKey(713,"01.01.00.00.00.00"),713);
		treeMap.put(new TreeMapKey(5750,"01.01.01.00.00.00"),5750);
		treeMap.put(new TreeMapKey(7275,"01.01.02.00.00.00"),7275);
		treeMap.put(new TreeMapKey(2958,"01.01.03.00.00.00"),2958);
		treeMap.put(new TreeMapKey(1316,"01.01.04.00.00.00"),1316);
		Iterator titer = treeMap.entrySet().iterator();
		while (titer.hasNext()) {
			Map.Entry en = (Map.Entry) titer.next();
			TreeMapKey mapKey = (TreeMapKey) en.getKey();
			String cateId = mapKey.getCateId();
			int count = mapKey.getCount();
			System.out.println(cateId+"count"+count);
		}
	}

}
