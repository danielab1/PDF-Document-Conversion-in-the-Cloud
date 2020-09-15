import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Client {



    private static class JobComparator implements Comparator<Job> {
        @Override
        public int compare(Job o1, Job o2) {
            return o2.leftToComplete()-o1.leftToComplete();
        }
    }
    private Map<String, Job> jobsMap;
    private String id;
    private String queueUrl;

    public Client(String clientId, String queueUrl){
        this.id = clientId;
        this.jobsMap = new ConcurrentHashMap<>();
        this.queueUrl = queueUrl;
    }


    public Map<String, Job> getJobsMap() {
        return jobsMap;
    }

    public String getId() {
        return id;
    }

    public String getQueueUrl(){
        return queueUrl;
    }

    public int markTaskCompleted(String jobId, String taskId, String success, String res){
        Job j = jobsMap.get(jobId);
        Task t = j.getTask(taskId);
        return j.taskCompleted(t, success, res);
    }



    public synchronized void addJob(Job j){
        jobsMap.put(String.valueOf(j.getJobId()), j);
    }

    public String toString(){
        String s = "clint id: " + id;
        s += "\nqueue url: " + queueUrl;
        s += "\njobs queue: \n";
        for(Map.Entry<String,Job> entry : jobsMap.entrySet()){
            s += "\n" + entry.getValue().toString() + "\n";
        }
        return s;
    }
}
