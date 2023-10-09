package cloud.workflowScheduling.idea.mpc_optSubD.mpc.compaired;

import java.util.*;

import cloud.workflowScheduling.methods.Scheduler;
import cloud.workflowScheduling.setting.*;

//修正后的HEFT, 没有平均执行时间，是否启动新机器用ProbLiS的方法
public class GRP_HEFT implements Scheduler{
	double pCheapest = Double.MAX_VALUE;
	
    public GRP_HEFT() {
    	
    }
    
	public Solution schedule(Workflow wf) {
		pCheapest = Double.MAX_VALUE;
		VM.resetInternalId();
		
		wf.calcTaskLevels();	
		List<Task> tasks = new ArrayList<Task>(wf);
		Collections.sort(tasks, new Task.BLevelComparator()); 	
		Collections.reverse(tasks);	//sort based on pURank, larger first
		
		List<VM> instTypeList = new ArrayList<VM>(); //非支配实例列表
		//得到非支配实例
		VM[] vmPool = new VM[VM.TYPE_NO];
		for(int i = 0; i < VM.TYPE_NO; i++){
			vmPool[i] = new VM(i);
		}
		for(int i = 0; i < VM.TYPE_NO; i++){
			boolean dominateI = false;
			for(int j = 0; j < VM.TYPE_NO; j++){
				if(i == j)
					continue;
				else {
					if((vmPool[j].getUnitCost() <= vmPool[i].getUnitCost()) && (vmPool[j].getSpeed() >= vmPool[i].getSpeed()))
						dominateI = true;
				}
			}
			if(!dominateI)
				instTypeList.add(vmPool[i]);
		}
		//释放被支配的实例
		VM.setInternalId(instTypeList.size());
		vmPool = null;
		
		//将instTypeList按照效率排序
		for(VM vm : instTypeList)
			vm.setEffRate(vm.getSpeed()/vm.getUnitCost());
		Collections.sort(instTypeList, new VM.EffRateComparator()); 	
		Collections.reverse(instTypeList);	//sort based on pURank, larger first
		
		//找到instTypeList中最便宜VM的单元收费
		for(VM vm : instTypeList)
			if(vm.getUnitCost() < this.pCheapest)
				this.pCheapest = vm.getUnitCost();
		
		return run(wf, tasks, instTypeList);
	}
	
	public Solution run(Workflow wf, List<Task> tasks, List<VM> instTypeList) {
		
		Solution globalBestSol = null;		
		
		int iterTime = 0;
		while(!instTypeList.isEmpty()) {
			//资源预置
			double remainingBudget = wf.getBudget();
			List<VM> usedVMPool = new ArrayList<VM>(); //Y
			int i = 0;
			while((i < instTypeList.size()) && (remainingBudget >= this.pCheapest)) {
				VM vm = instTypeList.get(i);
				int num = (int)Math.floor(remainingBudget/vm.getUnitCost());
				for(int j = 0; j < num; j++) {
					if(j == 0)
						usedVMPool.add(vm);
					else
						usedVMPool.add(new VM(vm.getType()));
				}
				i++;
				remainingBudget -= vm.getUnitCost()*num;
			}
			
			//指派：Modified-HEFT
			Solution s = this.modified_HEFT(tasks, usedVMPool);
			
			//更新全局最好解
			if(iterTime == 0)
				globalBestSol = s;
			else if(s.calcCost() <= wf.getBudget() && s.calcMakespan() <= globalBestSol.calcMakespan())
				globalBestSol = s;
			instTypeList.remove(0);
			usedVMPool = null;
			VM.setInternalId(instTypeList.size());
			
			iterTime++;
		}
		
		return globalBestSol;
	}
	
	private Solution modified_HEFT(List<Task> tasks, List<VM> usedVMPool){
		Solution solution = new Solution();

		for(int i = 1; i < tasks.size(); i++){		
			Task task = tasks.get(i);

			Allocation alloc = getBestVM(task, solution, usedVMPool, i);

			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVM(alloc.getVM(), tasks.get(0), alloc.getStartTime(), true);
			solution.addTaskToVM(alloc.getVM(), task, alloc.getStartTime(), true);	//allocate
		}
		
		return solution;
	}
	
	private Allocation getBestVM(Task task, Solution solution, List<VM> usedVMPool, int taskIndex){
		VM selectedVM = null, Imin = null, IminStar = null; //最终选择的VM, 最早完成时间的机器，不增加花费最早完成时间的机器				
		double selectedStartTime = 0, selectedStartTimeStar = 0;
		double minEFT = Double.MAX_VALUE, minEFTStar = Double.MAX_VALUE;
		boolean isAssigned = false;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		for(VM vm : usedVMPool){	
			if(taskIndex == 1)
				startTime = VM.LAUNCH_TIME;
			else if(!solution.containsKey(vm))
				startTime = solution.calcEST(task, null);
			else
				startTime = solution.calcEST(task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < minEFT){
				minEFT = finishTime;
				Imin = vm;
				selectedStartTime = startTime;
			}
			
			double increasedCost = Double.MAX_VALUE;
			if(solution.containsKey(vm)) {
				double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);
				double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
				increasedCost = newVMTotalCost - solution.calcVMCost(vm);
			}
			else
				increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * vm.getUnitCost();
			if(increasedCost == 0 && finishTime < minEFTStar){
				minEFTStar = finishTime;
				IminStar = vm;
				selectedStartTimeStar = startTime;
			}
		}
		
		VM deleteVM = null;
		if(!Imin.equals(IminStar)) {
			if(solution.containsKey(Imin)) {
				for(VM v : usedVMPool) {
					if((Imin.getType() == v.getType()) && (!solution.containsKey(v))) {
						deleteVM = v;
						break;
					}
				}
			}
			else if(IminStar != null) {
				isAssigned = true;
				selectedVM = IminStar;
				selectedStartTime = selectedStartTimeStar;
			}
		}
		if(deleteVM != null)
			usedVMPool.remove(deleteVM);
		if(!isAssigned) {
			selectedVM = Imin;
		}

		return  new Allocation(selectedVM, task, selectedStartTime);
	}

}