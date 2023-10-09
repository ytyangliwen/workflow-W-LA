package cloud.workflowScheduling.setting;

import java.io.Serializable;
import java.util.Comparator;

// virtual machine, i.e., cloud service resource
public class VM implements Serializable{

	public static final double LAUNCH_TIME = 0; //Amazon VM启动时间97s
	public static final long NETWORK_SPEED = 20 * 1024*1024; //带宽20MB
	
	//L-ACO中的
//	public static final int TYPE_NO = 9;
//	public static final double[] SPEEDS = {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5};
//	public static final double[] UNIT_COSTS = {0.12, 0.195, 0.28, 0.375, 0.48, 0.595, 0.72, 0.855, 1};
//	public static final double INTERVAL = 3600;	//one hour, billing interval
//	public static final int FASTEST = 8;
//	public static final int SLOWEST = 0;
	
	//文章中常用的, 上一代实例, in the USA East region 
//	public static final int TYPE_NO = 6;
////								m1.small, m1.medium, m1.large, m1.xlarge, m3.xlarge, m3.2xlarge		
//	public static final double[] SPEEDS = {4400, 8800, 17600, 35200, 57200, 114400}; //MFLOPS
//	public static final double[] UNIT_COSTS = {0.06, 0.12, 0.24, 0.48, 0.5, 1};
////	public static final double[] Bandwidth = {39321600, 85196800, 85196800, 131072000, 131072000, 131072000} //bytes
//	public static final double INTERVAL = 3600;	//one hour, billing interval
//	public static final int FASTEST = 5;
//	public static final int SLOWEST = 0;
	
	//文章中常用的, Budget and Deadline文章, in the USA East region[US East(N. Virginia)] 
	//deadlineFactor 4, 8, 12, 16
//	public static final int TYPE_NO = 6;
////									m3.medium, m4.large, m3.xlarge, m4.2xlarge, m4.4xlarge, m4.10xlarge		
//	public static final double[] SPEEDS = {3, 6.5, 13, 26, 53.5, 124.5}; //ECU
//	public static final double[] UNIT_COSTS = {0.067, 0.126, 0.266, 0.504, 1.008, 2.520}; //$
////	public static final double[] Bandwidth = {3.75, 8, 15, 32, 64, 160} //GB
//	public static final double INTERVAL = 3600;	//one hour, billing interval
//	public static final int FASTEST = 5;
//	public static final int SLOWEST = 0;
	
	//文章中常用的, Minimizing文章, in the USA East region[US East(N. Virginia)] 
	//deadlineFactor 1.5, 2, 3, 4, ..., 9, 10
	public static final int TYPE_NO = 8;
////									m1.small, m1.medium, m3.medium, m1.large, m3.large, m1.xlarge, m3.xlarge, m3.2xlarge		
	public static final double[] SPEEDS = {1, 2, 3, 4, 6.5, 8, 13, 26,}; //ECU
	public static final double[] UNIT_COSTS = {0.044, 0.087, 0.067, 0.175, 0.133, 0.35, 0.266, 0.532}; //$
//	public static final double[] Bandwidth = {3.75, 7.5, 15, 30, 1.7, 3.75, 7.5, 15} //GB
	public static final double INTERVAL = 3600;	//one hour, billing interval
	public static final int FASTEST = 7;
	public static final int SLOWEST = 0;
//	
	//mpc-Sample 说明subD的粒子
	//deadlineFactor 3
//	public static final int TYPE_NO = 3;
////									m1.small, m1.medium, m3.medium, m1.large, m3.large, m1.xlarge, m3.xlarge, m3.2xlarge		
//	public static final double[] SPEEDS = {3, 13, 26}; //ECU
//	public static final double[] UNIT_COSTS = {0.067, 0.266, 0.532}; //$
////	public static final double[] Bandwidth = {3.75, 7.5, 15, 30, 1.7, 3.75, 7.5, 15} //GB
//	public static final double INTERVAL = 3600;	//one hour, billing interval
//	public static final int FASTEST = 2;
//	public static final int SLOWEST = 0;
	
	//mpc-Sample 说明mpc的粒子
	//deadlineFactor 3
//	public static final int TYPE_NO = 2;
////									m1.small, m1.medium, m3.medium, m1.large, m3.large, m1.xlarge, m3.xlarge, m3.2xlarge		
//	public static final double[] SPEEDS = {1, 2}; //ECU
//	public static final double[] UNIT_COSTS = {1, 1.5}; //$
////	public static final double[] Bandwidth = {3.75, 7.5, 15, 30, 1.7, 3.75, 7.5, 15} //GB
//	public static final double INTERVAL = 3600;	//one hour, billing interval
//	public static final int FASTEST = 1;
//	public static final int SLOWEST = 0;
	
	private static int internalId = 0;
	public static void resetInternalId(){	//called by the constructor of Solution
		internalId = 0;
	}
	public static void setInternalId(int startId){
		internalId = startId;
	}
	
	private int id;
	private int type; 
	//GRP-HEFT
	private double effRate;

	public VM(int type){
		this.type = type;
		this.id = internalId++;
	}
	
//	public int hashCode()
//	{
//		final int NUM = 23;
//		return id*NUM;
//	}
//	public boolean equals(Object obj)
//	{
//		if(this==obj)
//			return true;
//		if(!(obj instanceof VM))
//			return false;
//		VM stu = (VM)obj;
//		return this.id==stu.id;
//	}
	//------------------------getters && setters---------------------------
	public void setType(int type) {		//can only be invoked in the same package, e.g., Solution
		this.type = type;
	}
	public void setId(int id) {
		this.id = id;
	}
	public double getSpeed(){		return SPEEDS[type];	}
	public double getUnitCost(){		return UNIT_COSTS[type];	}
	public int getId() {		return id;	}
	public int getType() {		return type;	}
	
	public void setEffRate(double effRate) {	
		this.effRate = effRate;
	}
	public double getEffRate() {	
		return this.effRate;
	}
	
	//-------------------------------------overrides--------------------------------
	public String toString() {
		return "VM [id=" + id + ", type=" + type + "]";
	}
	
	
	public static class EffRateComparator implements Comparator<VM>{
		public int compare(VM o1, VM o2) {
			if(o1.getEffRate() > o2.getEffRate())
				return 1;
			else if(o1.getEffRate() < o2.getEffRate())
				return -1;
			else{
				if(o1.getSpeed() > o2.getSpeed())
					return 1;
				else if(o1.getSpeed() < o2.getSpeed())
					return -1;
				else
					return 0;
			}
		}
	}
}