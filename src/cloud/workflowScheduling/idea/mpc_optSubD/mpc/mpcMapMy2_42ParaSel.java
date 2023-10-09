package cloud.workflowScheduling.idea.mpc_optSubD.mpc;

import java.io.IOException;
import java.util.*;

import cloud.workflowScheduling.*;
import cloud.workflowScheduling.methods.Scheduler;
import cloud.workflowScheduling.setting.*;

//参数选择：所有任务预测到底
public class mpcMapMy2_42ParaSel implements Scheduler {
	private double theta = 0.5;
	private int workflowDepth; //即入口任务的depth
	private Map<Integer, List<Task>> taskLevelSet;
	private Map<Integer, Double> levelWidthTrend; //level宽度的变化趋势，theta^(L.size/(L-1).size), 当前level包含的任务个数/下一层level包含的任务个数
	
	private int predictTaskNum = 4;
	private int enrySize = 0;
	private Workflow wf = null;

	public mpcMapMy2_42ParaSel(double theta){
		this.theta = theta;
	}
	public mpcMapMy2_42ParaSel(){

	}
	
	public Solution schedule(Workflow wf) {
		//调度前，先初始化成员变量
		workflowDepth = 0; //即入口任务的depth
		taskLevelSet = new LinkedHashMap<Integer, List<Task>>();
		levelWidthTrend = new LinkedHashMap<Integer, Double>();
		
		this.wf = wf;
		Task entryTask = wf.get(0);
		
		//计算任务depth, 从出口任务开始
		List<Task> exits = new ArrayList<Task>();
		for(Task t : wf) {
			if(t.getOutEdges().isEmpty())
				exits.add(t);
		}
		for (Iterator it = exits.iterator(); it.hasNext();) {
            Task task = (Task) it.next();
            task.setDepthFromExit(task, 1);
        }	
		this.workflowDepth = entryTask.getDepth();
		
		//计算TLS
		for(int i = this.workflowDepth; i > 0; i--){	
			taskLevelSet.put(Integer.valueOf(i), new ArrayList<Task>());
		}
		for(int i = 0; i < wf.size(); i++){
			Task task = wf.get(i);
			int depth = task.getDepth();
			taskLevelSet.get(Integer.valueOf(depth)).add(task);
		}
		
		//计算level宽度的变化趋势
		for(int i = this.workflowDepth; i > 2; i--){
			double rate = Math.pow(this.theta, taskLevelSet.get(Integer.valueOf(i)).size()/taskLevelSet.get(Integer.valueOf(i-1)).size());
			levelWidthTrend.put(Integer.valueOf(i), Double.valueOf(rate));
		}
		levelWidthTrend.put(Integer.valueOf(1), Double.valueOf(0.0));
		levelWidthTrend.put(Integer.valueOf(2), Double.valueOf(0.0));
		
		wf.calcPURank241(taskLevelSet, levelWidthTrend);	
		
		List<Task> tasks = new ArrayList<Task>(wf);
		Collections.sort(tasks, new Task.PURankComparator()); 	
		Collections.reverse(tasks);	//sort based on pURank, larger first
		
		Solution s = null;
		try {
			s = buildViaTaskList(wf, tasks, wf.getDeadline());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return s;
	}
	
	//build a solution based on a task ordering.
	//that is, for a given task ordering, distribute deadline and select services here
	public Solution buildViaTaskList(Workflow wf, List<Task> tasks, double deadline) throws ClassNotFoundException, IOException {
//		try {
//			EvaluateYLW.subD[0].write(String.format("%15s", "mpcMapMy2_3: "));
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
		int violationCount = 0;		// test code
		Solution solution = new Solution();
		double CPLength = wf.get(0).getpURank(); 	//critical path
		
		//输出各层的个数
//		enrySize = 1;
//		for(int i = 2; i < tasks.size(); i++) {
//			if(tasks.get(i).getDepth() == tasks.get(i-1).getDepth())
//				enrySize++; 
//			else {
//				System.out.print("[" + enrySize + ", " + tasks.get(i-1).getDepth() + "]; ");
//				enrySize = 1;
//			}
//		}
		
		enrySize = 0;
		for(int i = 1; i < tasks.size(); i++) {
			if(tasks.get(i).getDepth() == (this.workflowDepth-1))
				enrySize++; //rank前面中入口任务的个数
			else
				break;
		}
//		enrySize = this.taskLevelSet.get(Integer.valueOf(this.workflowDepth-1)).size();
//		if(enrySize > (int)(tasks.size() * 0.1))
//			enrySize = (int)(tasks.size() * 0.1);
//		if(enrySize < 3)
//			enrySize = 3;
//		predictTaskNum = enrySize; //mpcMap的入口任务预测entrySize布
		
		//计算每个任务的subD
		for(int i = 1; i < tasks.size(); i++){	
			Task task = tasks.get(i);
			double proSubDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
					/CPLength * deadline;
			task.setSubD(proSubDeadline);
		}
		
		predictTaskNum = tasks.size(); //所有任务预测到底
		for(int i = 1; i < tasks.size(); i++){	
			Task task = tasks.get(i);
			
			//从当前任务向后预测predictTaskNum个任务，计算cost
			List<Task> predictTasks = new ArrayList<Task>();
			
//			if(i <= (int)(tasks.size() * 0.1)) { //前10%的任务预测到底
//				predictTaskNum = tasks.size(); 
//			}
			
//			if(task.getDepth() == (this.workflowDepth-1)) { //入口任务预测到底
////				if(i <= enrySize) {
//					predictTaskNum = tasks.size();
////					if(predictTaskNum < (int)(tasks.size() * 0.1))
////						predictTaskNum = (int)(tasks.size() * 0.1);
////					else if(predictTaskNum > (int)(tasks.size() * 0.2))
////						predictTaskNum = (int)(tasks.size() * 0.2);
//				}
//				else
//					predictTaskNum = 0;
			
			for(int j = i+1; (j < (i+this.predictTaskNum+1)) && (j<tasks.size()); j++) //mpcMap的预测步数固定为workflowsize*0.1
				predictTasks.add(tasks.get(j));
					
//			double proSubDeadline = (CPLength - task.getpURank() + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
//							/CPLength * deadline;
			double proSubDeadline = task.getSubD();
			
//			task.setSubD(proSubDeadline);
//			try {
//				EvaluateYLW.subD[0].write(task.getId() + ": " + String.format("%.3f", proSubDeadline) + ", ");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			Allocation alloc = getMinCostVM(task, solution,proSubDeadline, i);

			Allocation alloc = getPredictedMinCostVM(task, predictTasks, solution,proSubDeadline, i);
			

			//当CPLength>deadline时，子期限的划分可能导致EFT>subDeadline；所以必须考虑子期限不满足的情况：此时选择minimal EFT的VM
			if(alloc == null){			//select a vm which allows EFT
				alloc = getMinEFTVM(task, solution, proSubDeadline, i);
				
//				VM vm = alloc.getVM();
//				while(alloc.getFinishTime() > proSubDeadline + Evaluate.E && vm.getType() < VM.FASTEST){
//					solution.updateVM(vm);			//upgrade若进行整个解的更新；复杂度将增长太多。
//					alloc.setStartTime(solution.calcEST(task, vm));
//					alloc.setFinishTime(solution.calcEST(task, vm) + task.getTaskSize()/vm.getSpeed());
//				}
				if(alloc.getFinishTime() > proSubDeadline + Evaluate.E)
					violationCount ++;
			}
			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVM(alloc.getVM(), tasks.get(0), alloc.getStartTime(), true);
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);	//allocate
			
//			HashMap<Task, Allocation> remap = solution.getRevMapping();
//			Allocation a = remap.get(task);
//			task.calDRank(solution);
////			newCPLength = task.getDRank() + (task.getbLevel() + task.getTaskSize()/alloc.getVM().getSpeed() - task.getFastET());
//			newCPLength = task.getDRank() + task.getbLevel();
			
			//更新已完成任务的rank值，进而更新subD
//			task.updateURank(solution);
//			task.updateParentsURank(solution); 

		}
//		try {
//			EvaluateYLW.subD[0].write("\r\n");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		if(violationCount > 0)
//			System.out.println("Number of sub-deadline violation: " + violationCount);
		
		return solution;
	}
	
	// select a vm that meets sub-deadline and minimizes the cost
	//candidate services include all the services that have been used (i.e., R), 
	//			and those that have not been used but can be added any time (one service for each type)
	private Allocation getMinCostVM(Task task, Solution solution, double subDeadline, int taskIndex){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		for(VM vm : solution.keySet()){	
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime > subDeadline + Evaluate.E)   //sub-deadline not met
				continue;
			
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
			double increasedCost = newVMTotalCost - solution.calcVMCost(vm);  // oldVMTotalCost
			if(increasedCost < minIncreasedCost){ 
				minIncreasedCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];
			if(finishTime > subDeadline + Evaluate.E)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < minIncreasedCost){
				minIncreasedCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		
		if(selectedVM == null)
			return null;
		else
			return new Allocation(selectedVM, task, selectedStartTime);
	}
	
	private Allocation getPredictedMinCostVM(Task task, List<Task> predictTasks, Solution solution, double subDeadline, int taskIndex) throws ClassNotFoundException, IOException{
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
//		double maxOutTime = 0;	//maxTransferOutTime
//		for(Edge e : task.getOutEdges())
//			maxOutTime = Math.max(maxOutTime, e.getDataSize());
//		maxOutTime /= VM.NETWORK_SPEED;
		
		Solution solutionCopy = null;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		int vmSize = solution.size();
		for(VM vm : solution.keySet()){
			solutionCopy = Solution.deepcopy(solution);
			
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			Allocation alloc = new Allocation(vm, task, startTime);
			if(alloc.getFinishTime() > subDeadline + Evaluate.E)
				continue;
			solutionCopy.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);	//allocate
			
			int predictTaskIndex = taskIndex;
			for(Task predictTask : predictTasks) { 
				predictTaskIndex ++;
//				double CPLength = this.wf.get(0).getpURank();
//				double predictTaskSubDeadline = (CPLength - predictTask.getpURank() + predictTask.getTaskSize()/VM.SPEEDS[VM.FASTEST])
//						/CPLength * this.wf.getDeadline();
				double predictTaskSubDeadline = predictTask.getSubD();
				
				Allocation PreAlloc = getMinCostVM(predictTask, solutionCopy, predictTaskSubDeadline, predictTaskIndex);
				//当CPLength>deadline时，子期限的划分可能导致EFT>subDeadline；所以必须考虑子期限不满足的情况：此时选择minimal EFT的VM
				if(PreAlloc == null){			//select a vm which allows EFT
					PreAlloc = getMinEFTVM(predictTask, solutionCopy, predictTaskSubDeadline, predictTaskIndex);
//					VM preVm = PreAlloc.getVM();
//					while(PreAlloc.getFinishTime() > predictTaskSubDeadline + Evaluate.E && preVm.getType() < VM.FASTEST){
//						solutionCopy.updateVM(preVm);			//upgrade若进行整个解的更新；复杂度将增长太多。
//						PreAlloc.setStartTime(solutionCopy.calcEST(task, preVm));
//						PreAlloc.setFinishTime(solutionCopy.calcEST(task, preVm) + task.getTaskSize()/preVm.getSpeed());
//					}
				}
				solutionCopy.addTaskToVM(PreAlloc.getVM(), predictTask, PreAlloc.getStartTime(), true);	//allocate
			}

			double cost = solutionCopy.calcCost();  // oldVMTotalCost
			if(cost < minIncreasedCost){ 
				minIncreasedCost = cost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
			
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			solutionCopy = Solution.deepcopy(solution);
			
			VM newVM = new VM(k);
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];
			Allocation alloc = new Allocation(newVM, task, startTime);
			if(alloc.getFinishTime() > subDeadline + Evaluate.E)
				continue;
			if(taskIndex == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solutionCopy.addTaskToVM(alloc.getVM(), this.wf.get(0), alloc.getStartTime(), true);
			solutionCopy.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);	//allocate
			
			
			int predictTaskIndex = taskIndex;
			for(Task predictTask : predictTasks) { 
				predictTaskIndex ++;
//				double CPLength = this.wf.get(0).getpURank();
//				double predictTaskSubDeadline = (CPLength - predictTask.getpURank() + predictTask.getTaskSize()/VM.SPEEDS[VM.FASTEST])
//						/CPLength * this.wf.getDeadline();
				double predictTaskSubDeadline = predictTask.getSubD();
				
				Allocation PreAlloc = getMinCostVM(predictTask, solutionCopy, predictTaskSubDeadline, predictTaskIndex);
				//当CPLength>deadline时，子期限的划分可能导致EFT>subDeadline；所以必须考虑子期限不满足的情况：此时选择minimal EFT的VM
				if(PreAlloc == null){			//select a vm which allows EFT
					PreAlloc = getMinEFTVM(predictTask, solutionCopy, predictTaskSubDeadline, predictTaskIndex);
//					VM preVm = PreAlloc.getVM();
//					while(PreAlloc.getFinishTime() > predictTaskSubDeadline + Evaluate.E && preVm.getType() < VM.FASTEST){
//						solutionCopy.updateVM(preVm);			//upgrade若进行整个解的更新；复杂度将增长太多。
//						PreAlloc.setStartTime(solutionCopy.calcEST(task, preVm));
//						PreAlloc.setFinishTime(solutionCopy.calcEST(task, preVm) + task.getTaskSize()/preVm.getSpeed());
//					}
				}
				solutionCopy.addTaskToVM(PreAlloc.getVM(), predictTask, PreAlloc.getStartTime(), true);	//allocate
			}

			double cost = solutionCopy.calcCost();  // oldVMTotalCost
			if(cost < minIncreasedCost){ 
				minIncreasedCost = cost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		
		//释放所有新增加的VM
		VM.setInternalId(vmSize);
		
		if(selectedI != -1)
			selectedVM = new VM(selectedI);
		
		if(selectedVM == null)
			return null;
		else
			return new Allocation(selectedVM, task, selectedStartTime);
	}
	
	//select a VM from R which minimizes the finish time of the task
	//here, candidates only include services from R if R is not null
	private Allocation getMinEFTVM(Task task, Solution solution, double subDeadline, int taskIndex){
		VM selectedVM = null;				
		double selectedStartTime = 0;
		double minEFT = Double.MAX_VALUE;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that minimizes EFT
		for(VM vm : solution.keySet()){			
			startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		// if solution has no VMs 
		if(selectedVM==null ){		// logically, it is equal to "solution.keySet().size()==0"
			startTime = taskIndex==1 ? VM.LAUNCH_TIME : solution.calcEST(task, null);
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[VM.FASTEST];
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedStartTime = startTime;
				selectedVM = new VM(VM.FASTEST);
			}
		}
		return  new Allocation(selectedVM, task, selectedStartTime);
	}
}