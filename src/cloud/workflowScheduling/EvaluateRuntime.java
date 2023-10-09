package cloud.workflowScheduling;

import java.io.*;

import cloud.workflowScheduling.idea.mpc_optSubD.mpc.mpcMapMy2_42;
import cloud.workflowScheduling.methods.*;
import cloud.workflowScheduling.setting.*;

//mpc算法计算执行时间用的
/**
 * 计算相同size下，每种算法在不同类型的工作流上的平均执行时间
 * @author Wen
 *
 */
public class EvaluateRuntime {
	static final String WORKFLOW_LOCATION = "E:\\0Work\\workspace-notUTF-8\\L-ACO-paper\\workflowSamples";
	static final String OUTPUT_LOCATION = ".\\result";
	
//	private static final double DEADLINE_FACTOR = 0.2; 
	private static double DF_START = 1, DF_INCR = 1, DF_END = 20;
	private static final int FILE_INDEX_MAX = 10;  //10
	private static final String[] WORKFLOWS = { "GENOME", "CYBERSHAKE", "LIGO", "MONTAGE", "SIPHT"};//

	private static final Scheduler[] METHODS = {
			new ICPCP(),  new ProLiS(1.5), new PSO(), new LACO(), new mpcMapMy2_42(),
			}; 
	private static final int FILE_SIZE_MAX = 10;  //10
	
	public static void main(String[] args) throws Exception {
		
		long[][] runtime = new long[FILE_SIZE_MAX][METHODS.length];
//		for(int fileSizeIndex = 9; fileSizeIndex<FILE_SIZE_MAX; fileSizeIndex++){
		for(int fileSizeIndex = 9; fileSizeIndex>5; fileSizeIndex-= 3){
			int size = 100 * (fileSizeIndex+1);
			for(int methodIndex = 0; methodIndex < METHODS.length; methodIndex++){
				for(int typeIndex = 0;typeIndex<WORKFLOWS.length;typeIndex++){
					String workflow = WORKFLOWS[typeIndex];
					for(int fileNumIndex = 0;fileNumIndex<FILE_INDEX_MAX;fileNumIndex++){
						for (int di = 0; di <= (DF_END - DF_START) / DF_INCR; di++) { // deadline index
							String file = EvaluateRuntime.WORKFLOW_LOCATION + "\\" + workflow + 
									"\\" + workflow + ".n." + size + "." + fileNumIndex + ".dax";
	
							Workflow wf = new Workflow(file);	
							Benchmarks benSched = new Benchmarks(wf);
						
							double deadlineFactor = 0;
							if(di == 0)
								deadlineFactor = 1.5;
							else
								deadlineFactor = DF_START + DF_INCR * di;
	//						double deadline = benSched.getFastSchedule().calcMakespan() + (benSched.getCheapSchedule().calcMakespan()
	//								- benSched.getFastSchedule().calcMakespan())* DEADLINE_FACTOR;
							double deadline = benSched.getFastSchedule().calcMakespan() * deadlineFactor;
							wf.setDeadline(deadline);	
							
							long t1 = System.currentTimeMillis();
							METHODS[methodIndex].schedule(wf);
							runtime[fileSizeIndex][methodIndex] += System.currentTimeMillis() - t1;
						}
					}
				}
				runtime[fileSizeIndex][methodIndex] /= WORKFLOWS.length * FILE_INDEX_MAX * 20;
			}
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(EvaluateRuntime.OUTPUT_LOCATION + "\\runtime.txt", true));
			bw.write(size +"\t");
			for(int methodIndex = 0; methodIndex < METHODS.length; methodIndex++)
				bw.write(runtime[fileSizeIndex][methodIndex]+"\t"); 
			bw.write("\r\n");

			bw.flush();
			bw.close();
		}

		
//		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(EvaluateRuntime.OUTPUT_LOCATION + "\\runtime2.txt")));
//		for(int fileSizeIndex = 0; fileSizeIndex<FILE_SIZE_MAX; fileSizeIndex++){
//			int size = 100 * (fileSizeIndex+1);
//			bw.write(size +"\t");
//			for(int methodIndex = 0; methodIndex < METHODS.length; methodIndex++)
//				bw.write(runtime[fileSizeIndex][methodIndex]+"\t"); 
//			bw.write("\r\n");
//		}
//		bw.flush();
//		bw.close();
	}
}
