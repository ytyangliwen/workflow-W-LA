package cloud.workflowScheduling;

import java.io.*;
import java.math.*;
import java.util.*;

import org.apache.commons.math3.stat.*;

import cloud.workflowScheduling.idea.mpc_optSubD.mpc.*;
import cloud.workflowScheduling.idea.mpc_optSubD.mpc.compaired.*;
import cloud.workflowScheduling.methods.*;
import cloud.workflowScheduling.setting.*;

/*
 * û��FILE_INDEX_MAX, �ظ�ִ��10��
 * Please download the DAX workflow archive from 
 * https://download.pegasus.isi.edu/misc/SyntheticWorkflows.tar.gz, 
 * unzip it and keep the DAX workflows in an appropriate position before running.
 */
//��L-ACO��deadlineȡֵ
//EvaluateYLW_notLACO
public class EvaluateYLW {
	// because in Java float numbers can not be precisely stored, a very small
	// number E is added before testing whether deadline is met
	public static final double E = 0.0000001;

	private static double DF_START = 1, DF_INCR = 1, DF_END = 20;
	private static final int REPEATED_TIMES = 10;
	private static final int[] SIZES = { 50, 100, 1000};// 50, 100, 1000 task num of workflow

	private static final Scheduler[] METHODS = {
//			new mpcMapMy2_42ParaSel4(), new mpcMapMy2_42ParaSel(), new mpcMapMy2_42ParaSel2(), new mpcMapMy2_42ParaSel3(), 
			new ICPCP(),  new ProLiS(1.5), new PSO(), new LACO(), new mpcMapMy2_42(),
//			new ProLiS(1.5), new mpcProLiS(1.5), 
			}; 
	// "GENOME", "CYBERSHAKE", "LIGO", "MONTAGE" floodplain�������������У����һ�����
	private static final String[] WORKFLOWS = { "CyberShake", "Epigenomics", "LIGO", "Montage", "Sipht" }; 
//	 private static final String[] WORKFLOWS = {"Montage", };

	static final String WORKFLOW_LOCATION = ".\\dax";
	static final String OUTPUT_LOCATION = ".\\result";
	static final String ExcelFilePath = ".\\result\\solution.xls";
//	static final String SubDPath = ".\\result\\PSO�Ż�subD��50-100-1000\\subDeadline1-opSubD-PSO-proLiSURank-50-100-100.txt";
	public static ExcelManage em;
	public static String sheetName;
	public static boolean isPrintExcel = false; // true false

	public static BufferedWriter[] subD = new BufferedWriter[20];
	public static HashMap<String, HashMap<Integer, Double>> type2subD = new HashMap<String, HashMap<Integer, Double>>();

	public static void main(String[] args) throws Exception {
//		readOptimizedSubD(SubDPath);

		boolean flag = false; // false ֻ�н�Լ���� true ����һ��
		{
			for (int mi = 0; mi < METHODS.length; mi++) {
				subD[mi] = new BufferedWriter(new FileWriter(OUTPUT_LOCATION + "\\subDeadline" + mi + ".txt"));
			}

			if (isPrintExcel)
				ExcelManage.clearExecl(ExcelFilePath);
			int deadlineNum = (int) ((DF_END - DF_START) / DF_INCR + 1);
			for (String workflow : WORKFLOWS) {
				// three dimensions of these two arrays correspond to deadlines, methods, files,
				// respectively
				double[][][] successResult = new double[deadlineNum][METHODS.length][REPEATED_TIMES * SIZES.length];
				double[][][] NCResult = new double[deadlineNum][METHODS.length][REPEATED_TIMES * SIZES.length];
				double[] refValues = new double[4]; // store cost and time of fastSchedule and cheapSchedule

				for (int di = 0; di <= (DF_END - DF_START) / DF_INCR; di++) { // deadline index
					for (int si = 0; si < SIZES.length; si++) { // size index
						int size = SIZES[si];
						sheetName = workflow + "_" + size;
						if (isPrintExcel)
							em = ExcelManage.initExecl(ExcelFilePath, sheetName);
						for (int timeI = 0; timeI < REPEATED_TIMES; timeI++) { // workflow file index
							String file = WORKFLOW_LOCATION + "\\" + workflow + "_" + size + ".xml";
//							String file = WORKFLOW_LOCATION + "\\Sample\\" + workflow + ".xml";
							// String file = WORKFLOW_LOCATION + "\\" + workflow + ".xml";
							test(file, di, timeI, si, successResult, NCResult, refValues);
						}
					}
				}
				BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT_LOCATION + "\\" + workflow + ".txt"));
				bw.write("used methods: ");
				for (Scheduler s : METHODS)
					bw.write(s.getClass().getSimpleName() + "\t");
				bw.write("\r\n\r\n");
				printTo(bw, successResult, "success ratio");
				printTo(bw, NCResult, "normalized cost");

				bw.write("reference values (CF, MF, CC, MC)\r\n");
				double divider = SIZES.length * REPEATED_TIMES * deadlineNum;
				for (double refValue : refValues)
					bw.write(refValue / divider + "\t");
				bw.close();
			}
			for (int mi = 0; mi < METHODS.length; mi++) {
				subD[mi].close();
			}
		}
		if (flag) {
			DF_START = 1.0;
			DF_INCR = 1;
			DF_END = 10;

			for (int mi = 0; mi < METHODS.length; mi++) {
				subD[mi] = new BufferedWriter(new FileWriter(OUTPUT_LOCATION + "\\subDeadline" + mi + ".txt", true));
			}

			if (isPrintExcel)
				ExcelManage.clearExecl(ExcelFilePath);
			int deadlineNum = (int) ((DF_END - DF_START) / DF_INCR + 1);
			for (String workflow : WORKFLOWS) {
				// three dimensions of these two arrays correspond to deadlines, methods, files,
				// respectively
				double[][][] successResult = new double[deadlineNum][METHODS.length][REPEATED_TIMES * SIZES.length];
				double[][][] NCResult = new double[deadlineNum][METHODS.length][REPEATED_TIMES * SIZES.length];
				double[] refValues = new double[4]; // store cost and time of fastSchedule and cheapSchedule

				for (int di = 0; di <= (DF_END - DF_START) / DF_INCR; di++) { // deadline index
					for (int si = 0; si < SIZES.length; si++) { // size index
						int size = SIZES[si];
						sheetName = workflow + "_" + size;
						if (isPrintExcel)
							em = ExcelManage.initExecl(ExcelFilePath, sheetName);
						for (int timeI = 0; timeI < REPEATED_TIMES; timeI++) { // workflow file index
							String file = WORKFLOW_LOCATION + "\\" + workflow + "_" + size + ".xml";
							// String file = WORKFLOW_LOCATION + "\\" + workflow + ".xml";
							test(file, di, timeI, si, successResult, NCResult, refValues);
						}
					}
				}
				BufferedWriter bw = new BufferedWriter(
						new FileWriter(OUTPUT_LOCATION + "\\" + workflow + ".txt", true));
				bw.write("used methods: ");
				for (Scheduler s : METHODS)
					bw.write(s.getClass().getSimpleName() + "\t");
				bw.write("\r\n\r\n");
				printTo(bw, successResult, "success ratio");
				printTo(bw, NCResult, "normalized cost");

				bw.write("reference values (CF, MF, CC, MC)\r\n");
				double divider = SIZES.length * REPEATED_TIMES * deadlineNum;
				for (double refValue : refValues)
					bw.write(refValue / divider + "\t");
				bw.close();
			}
			for (int mi = 0; mi < METHODS.length; mi++) {
				subD[mi].close();
			}
		}
	}

	/**
	 * ����file������(�ڵ�si��size�£�fileIndex��)��deadlineΪdi�ɽ���ʱ���㷨�Ľ�����ɹ��ʣ���׼cost��
	 * 
	 * @param file
	 *            xml���ڵ��ļ�
	 * @param di
	 *            ��di��deadline���
	 * @param fi
	 *            �ظ�ִ��n�Σ����ǵ�fi��
	 * @param si
	 *            task size
	 * @param successResult
	 *            ��di�¸��㷨�ĳɹ���
	 * @param NCResult
	 *            normalized cost
	 * @param refValues
	 */
	private static void test(String file, int di, int fi, int si, double[][][] successResult, double[][][] NCResult,
			double[] refValues) {
		// ����file�ļ��еĹ�����
		Workflow wf = new Workflow(file);

		Benchmarks benSched = new Benchmarks(wf); // ��õ�ǰ������������Benchmark�⣬Ϊ�˼���max min��deadline
		System.out.print("Benchmark-FastSchedule��" + benSched.getFastSchedule());
		System.out.print("Benchmark-CheapSchedule��" + benSched.getCheapSchedule());
		System.out.print("Benchmark-MinCost8Schedule��" + benSched.getMinCost8Schedule());

		// ��ǰ��deadline = min+ (max-min)*deadlineFactor
		double deadlineFactor = 0;
		if(di == 0)
			deadlineFactor = 1.5;
		else
			deadlineFactor = DF_START + DF_INCR * di;
		double deadline = benSched.getFastSchedule().calcMakespan() * deadlineFactor;
		System.out.println("deadlineFactor=" + String.format("%.3f", deadlineFactor) + ", deadline = "
				+ String.format("%.3f", deadline));

		System.out.println();
		for (int mi = 0; mi < METHODS.length; mi++) { // method index
			Workflow wf1 = new Workflow(file);

			// ��ӡ������ִ��ʱ��ʹ���ʱ��
			/*
			 * try { BufferedWriter runT = new BufferedWriter(new FileWriter(OUTPUT_LOCATION
			 * + "\\runTime.txt")); BufferedWriter TT = new BufferedWriter(new
			 * FileWriter(OUTPUT_LOCATION + "\\TT.txt")); try { printRunTime(runT, wf1);
			 * printTT(TT, wf1); } catch (Exception e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); } } catch (IOException e1) { // TODO Auto-generated
			 * catch block e1.printStackTrace(); }
			 */

			Scheduler method = METHODS[mi];
			wf1.setDeadline(deadline);
			wf1.setDeadlineFactor(deadlineFactor); // ΪHGSA����
			
//			//Ϊ�������е�ÿ���������Ż��õ�subD
//			String type = file + "-deadlineFactor_" + String.format("%.3f", deadlineFactor);
//			HashMap<Integer, Double> task2SubD = type2subD.get(type);
//			for(Task t : wf1) {
//				if(t.getName() == "entry")
//					t.setSubD(0);
//				else if(t.getName() == "exit")
//					t.setSubD(deadline);
//				else {
////					System.out.println(t.getId());
//					t.setSubD(task2SubD.get(new Integer(t.getId())).doubleValue());
//				}
//			}
			
			String methodName = method.getClass().getCanonicalName();
			methodName = methodName.substring(methodName.lastIndexOf(".")+1);
			System.out.println("�����㷨The current algorithm: " + methodName);

			try {
				subD[mi].write(String.format("%55s", file + "-deadlineFactor_" + String.format("%.3f", deadlineFactor) + "\t"));
				 subD[mi].write("\r\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// �����㷨
			long starTime = System.currentTimeMillis();
			Solution sol = method.schedule(wf1);
			//// �鿴subDeadline
			// try {
			// subD[mi].write(String.format("%55s", file+"-deadlineFactor" + di+"-method"
			//// +"\t"));
			// subD.write("\r\n");
			// printSubDTo(subD[mi], wf1);
			// } catch (Exception e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			long endTime = System.currentTimeMillis();
			double runTime = (double) (endTime - starTime);

			if (isPrintExcel)
				em.writeToExcel(ExcelFilePath, sheetName, deadlineFactor, deadline, methodName, mi, sol);

			if (sol == null) {
				System.out.println("solution is " + sol + "!\r\n");
				continue;
			}
			int isSatisfied = sol.calcMakespan() <= deadline + E ? 1 : 0;
			List<Integer> result = sol.validateId(wf1);
			if (result.get(0).intValue() == 0) {
				if (isPrintExcel)
					em.writeToExcel(ExcelFilePath, sheetName, result.get(1).intValue(), result.get(0).intValue());
				throw new RuntimeException();
			}
			System.out.println("runtime��" + runTime + "ms;   solution: " + sol);

			//��ӡִ��ʱ��
			 try { 
				 BufferedWriter runT = new BufferedWriter(new FileWriter(OUTPUT_LOCATION + "\\runTime.txt", true)); 
				 try { 
					 runT.write(String.format("%70s", file + "-deadlineFactor_" + String.format("%.3f", deadlineFactor) +
							 " " + methodName + "��runtime��" + "\t") 
					  + runTime/1000 + "s");
					 runT.write("\r\n");
					 runT.close();
				 } catch (Exception e) { // TODO Auto-generated catch block
					 e.printStackTrace(); 
				 } 
			} catch (IOException e1) { // TODO Auto-generated
				e1.printStackTrace(); 
			}

			successResult[di][mi][fi + si * REPEATED_TIMES] += isSatisfied;
			NCResult[di][mi][fi + si * REPEATED_TIMES] += sol.calcCost() / benSched.getCheapSchedule().calcCost();
		}
		refValues[0] += benSched.getFastSchedule().calcCost();
		refValues[1] += benSched.getFastSchedule().calcMakespan();
		refValues[2] += benSched.getCheapSchedule().calcCost();
		refValues[3] += benSched.getCheapSchedule().calcMakespan();
	}

	private static final java.text.DecimalFormat df = new java.text.DecimalFormat("0.000");
	static {
		df.setRoundingMode(RoundingMode.HALF_UP);
	}

	private static void printTo(BufferedWriter bw, double[][][] result, String resultName) throws Exception {
		bw.write(resultName + "\r\n");
		for (int di = 0; di <= (DF_END - DF_START) / DF_INCR; di++) {
			String text = df.format(DF_START + DF_INCR * di) + "\t";
			for (int mi = 0; mi < METHODS.length; mi++)
				text += df.format(StatUtils.mean(result[di][mi])) + "\t";
			bw.write(text + "\r\n");
			bw.flush();
		}
		bw.write("\r\n\r\n\r\n");
	}

	private static void printSubDTo(BufferedWriter bw, Workflow w) throws Exception {
		// bw.write("\t");
		// for(Task t : w){
		// bw.write(String.format("%d", t.getId()) + ", ");
		// }
		// bw.write("\r\n\t");
		for (Task t : w) {
			bw.write(String.format("%.3f", t.getSubD()) + ", ");
		}
		bw.write("\r\n");
		bw.flush();
	}

	private static void printRunTime(BufferedWriter bw, Workflow w) throws Exception {

		for (Task t : w) {
			bw.write(String.valueOf(t.getId()));
			bw.write("\t");
			bw.write(String.valueOf(t.getTaskSize() / 4.4) + "\t");
			bw.write("\r\n");
		}
		bw.flush();
	}

	private static void printTT(BufferedWriter bw, Workflow w) throws Exception {
		int size = w.size();
		double[][] tt = new double[size][size];
		for (Task t : w) {
			int pId = t.getId();
			for (Edge e : t.getOutEdges()) {
				Task c = e.getDestination();
				int cId = c.getId();
				tt[pId][cId] = e.getDataSize() * 1.0 / VM.NETWORK_SPEED;
			}
		}

		for (int i = 0; i < size; i++) {
			for (int j = 0; j < size; j++) {
				bw.write(String.valueOf(tt[i][j]) + "\t");
			}
			bw.write("\r\n");
		}
		bw.flush();
	}

	private static void readOptimizedSubD(String filePath) {
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null; // ���ڰ�װInputStreamReader,��ߴ������ܡ���ΪBufferedReader�л���ģ���InputStreamReaderû�С�
		try {
			String str = "";
//			String str1 = "";
			String[] s, s1, s2;
			fis = new FileInputStream(filePath);// FileInputStream
			// ���ļ�ϵͳ�е�ĳ���ļ��л�ȡ�ֽ�
			isr = new InputStreamReader(fis);// InputStreamReader ���ֽ���ͨ���ַ���������,
			br = new BufferedReader(isr);// ���ַ��������ж�ȡ�ļ��е�����,��װ��һ��new InputStreamReader�Ķ���
			while ((str = br.readLine()) != null) {
//				str1 += str + "\n";
				HashMap<Integer, Double> task2SubD = new HashMap<Integer, Double>();
				s = str.split("\t");
				s1 = s[1].split(", ");
				for(int i = 0; i < s1.length; i ++) {
					s2 = s1[i].split(": ");
					task2SubD.put(Integer.valueOf(s2[0]), Double.valueOf(s2[1]));
				}
				type2subD.put(s[0].trim(), task2SubD);
			}
//			System.out.println(str1);// ��ӡ��str1
		} catch (FileNotFoundException e) {
			System.out.println("�Ҳ���ָ���ļ�");
		} catch (IOException e) {
			System.out.println("��ȡ�ļ�ʧ��");
		} finally {
			try {
				br.close();
				isr.close();
				fis.close();
				// �رյ�ʱ����ð����Ⱥ�˳��ر���󿪵��ȹر������ȹ�s,�ٹ�n,����m
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
//	/**
//	 * ��Readfile��subD, �ֱ�д��writeFile1��writeFile2
//	 * @param Readfile
//	 * @param writeFile1
//	 * @param  
//	 */
//	private static void readWriteOptimizedSubD(String Readfile, String writeFile1, String writeFile2) {
//		FileInputStream fis = null;
//		InputStreamReader isr = null;
//		BufferedReader br = null; // ���ڰ�װInputStreamReader,��ߴ������ܡ���ΪBufferedReader�л���ģ���InputStreamReaderû�С�
//		try {
//			String str = "";
////			String str1 = "";
//			String[] s, s1, s2;
//			fis = new FileInputStream(Readfile);// FileInputStream
//			// ���ļ�ϵͳ�е�ĳ���ļ��л�ȡ�ֽ�
//			isr = new InputStreamReader(fis);// InputStreamReader ���ֽ���ͨ���ַ���������,
//			br = new BufferedReader(isr);// ���ַ��������ж�ȡ�ļ��е�����,��װ��һ��new InputStreamReader�Ķ���
//			while ((str = br.readLine()) != null) {
////				str1 += str + "\n";
//				HashMap<Integer, Double> task2SubD = new HashMap<Integer, Double>();
//				s = str.split("\t");
//				s1 = s[1].split(", ");
//				for(int i = 0; i < s1.length; i ++) {
//					s2 = s1[i].split(": ");
//					task2SubD.put(Integer.valueOf(s2[0]), Double.valueOf(s2[1]));
//				}
//				type2subD.put(s[0].trim(), task2SubD);
//			}
////			System.out.println(str1);// ��ӡ��str1
//		} catch (FileNotFoundException e) {
//			System.out.println("�Ҳ���ָ���ļ�");
//		} catch (IOException e) {
//			System.out.println("��ȡ�ļ�ʧ��");
//		} finally {
//			try {
//				br.close();
//				isr.close();
//				fis.close();
//				// �رյ�ʱ����ð����Ⱥ�˳��ر���󿪵��ȹر������ȹ�s,�ٹ�n,����m
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//
//	}
	
}