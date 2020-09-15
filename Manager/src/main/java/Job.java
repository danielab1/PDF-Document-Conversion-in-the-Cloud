import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Job{
    private AtomicInteger jobsRequested;
    private AtomicInteger jobsDone;
    private int jobId;
    private String outputName;
    private Map<String, Task> tasksMap;
    private Boolean allJobReceived;

    public Job(int jobId, String outputName){
        this.jobsRequested = new AtomicInteger(0);
        this.jobsDone = new AtomicInteger(0);
        this.jobId = jobId;
        this.tasksMap = new HashMap<>();
        this.outputName = outputName;
        allJobReceived = false;
    }
    public void approveAllJobReceived(){
        allJobReceived = true;
    }
    public Boolean getAllJobReceived(){
        return allJobReceived;
    }
    public String getOutputName() {
        return outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    public Map<String, Task> getTasksMap() {
        return tasksMap;
    }

    public int getJobsRequested() {
        return jobsRequested.intValue();
    }

    public void setJobsRequested(AtomicInteger jobsRequested) {
        this.jobsRequested = jobsRequested;
    }

    public int getJobsDone() {
        return jobsDone.intValue();
    }

    public void setJobsDone(AtomicInteger jobsDone) {
        this.jobsDone = jobsDone;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public void addTask(Task t){
        jobsRequested.incrementAndGet();
        tasksMap.put(t.getTaskId(), t);
        ManagerRunner.balanceNumOfWorkers(1);
    }

    public Task getTask(String tId){
        return tasksMap.get(tId);
    }

    public int leftToComplete(){
        return jobsRequested.intValue()-jobsDone.intValue();
    }

    public int taskCompleted(Task t, String successStr, String res){
        boolean success = Boolean.parseBoolean(successStr);
        TASK_STATUS status = success ? TASK_STATUS.COMPLETED_SUCCESSFULLY : TASK_STATUS.COMPLETED_WITH_ERROR;

        if(t.getStatus() == TASK_STATUS.PENDING){
            this.jobsDone.incrementAndGet();
        }
        if(t.getStatus() == TASK_STATUS.COMPLETED_WITH_ERROR || t.getStatus() == TASK_STATUS.PENDING){
            t.setStatus(status);
            t.setDescription(res);
        }

        ManagerRunner.balanceNumOfWorkers(-1);
        return leftToComplete();
    }

    public String toString(){
        return "job id: " + jobId + ", jobs requested: " + jobsRequested.intValue() + ", jobs completed: " + jobsDone.intValue();
    }
}