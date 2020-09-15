import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static java.lang.Thread.yield;

public class LocalReqHandler implements Runnable {
    private SQSToolkit sqsToolkit = SQSToolkit.getToolkit();
    PropertiesReader prop = new PropertiesReader(System.getProperty("user.dir")+"/configure.properties");

    private String localToManagerSqsUrl = sqsToolkit.getQueueURL(prop.getProperty("LOCALS_TO_MANAGER_QUEUE_NAME"));
    private String managerToWorkersUrl = sqsToolkit.getQueueURL(prop.getProperty("MANAGER_TO_WORKERS_QUEUE"));

    private static Map<String, Client> clientsMap = ManagerRunner.clientsMap;
    private static AtomicInteger jobIdsCounter = ManagerRunner.jobIdsCounter;
    private static AtomicInteger taskIdsCounter = ManagerRunner.taskIdsCounter;

    @Override
    public void run() {
        /* ---- FETCH INCOMING LOCALS ---- */
        List<Message> messages = sqsToolkit.getQueueMessages(localToManagerSqsUrl);
        while (!ManagerRunner.terminate) {
            handleMemoryUsage();
            System.out.println("num of messages: " + messages.size());
            for (Message m : messages) {
                System.out.println("the body: " + m.getBody());
                if (m.getBody().equals("terminate")) {
                    ManagerRunner.terminate = true;
                    sqsToolkit.deleteMsgFromQueue(localToManagerSqsUrl, m);
                    break;
                }
                sqsToolkit.deleteMsgFromQueue(localToManagerSqsUrl, m);
                handleMsg(m);
            }
            messages = sqsToolkit.getQueueMessages(localToManagerSqsUrl);
            yield();
        }


        System.out.printf(" the Thread %s is exit now from local listener\n" , Thread.currentThread().getName());
    }

    private void handleMsg(Message msg) {
        S3Toolkit s3Toolkit = S3Toolkit.getToolkit();
        Map<String, MessageAttributeValue> attrs = msg.getMessageAttributes();
        String filename = attrs.get("filename").getStringValue();
        String outputName = attrs.get("outputName").getStringValue();
        String clientId = attrs.get("senderId").getStringValue();
        int workersPerNTasks = Integer.parseInt(attrs.get("workersPerNTasks").getStringValue());

        ManagerRunner.workersPerNTasks = workersPerNTasks < 0 ? 1 : workersPerNTasks;

        Client c = ManagerRunner.clientsMap.get(clientId);

        if (c == null) {
            String clientQueueUrl = attrs.get("localQueueUrl").getStringValue();
            c = new Client(clientId, clientQueueUrl);
            clientsMap.put(clientId, c);
            c = clientsMap.get(clientId);
        }
        if((float) c.getJobsMap().size()/ sqsToolkit.getNumOfMessages(localToManagerSqsUrl) > 0.1) {
            sqsToolkit.SendMsgToQueue(localToManagerSqsUrl, msg.getBody(), attrs);
            return;
        }
        Job job = new Job(jobIdsCounter.incrementAndGet(), outputName);
        c.addJob(job);
        String bucketName = attrs.get("bucketName").getStringValue();

        s3Toolkit.DownloadFile(bucketName, filename, line -> {
            Task t = parseTask(line, clientId, job.getJobId());
            job.addTask(t);
        });

        //Adding job to client only after making sure all tasks
        //were sent - in order to prevent a case where task is
        //done but the its id wasn't registered
        job.approveAllJobReceived();
        cleanFiles(filename);


    }
    private static void cleanFiles(String filename) {
        try {
            new File(filename).delete();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private Task parseTask(String line, String senderId, int jobId) {

        String[] split = line.split("\t");
        String cmd = split[0];
        String url = split[1];

        Task t = new Task("task" + taskIdsCounter.incrementAndGet(), url, cmd);

        System.out.println(line);
        Map<String, String> attrs = new HashMap<>();
        attrs.put("cmd", cmd);
        attrs.put("url", url);
        attrs.put("taskId", t.getTaskId());
        attrs.put("jobId", String.valueOf(jobId));
        sqsToolkit.SendMsgToQueue(managerToWorkersUrl, "new PDF task", attrs, senderId);

        return t;
    }
    private void handleMemoryUsage(){
        long freeSpace = new File("/").getFreeSpace();
        long totalSpace = new File("/").getTotalSpace();
        while((float)freeSpace/totalSpace < 0.05){
            try {
                sleep(100);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            freeSpace = new File("/").getFreeSpace();
            totalSpace = new File("/").getTotalSpace();
        }
    }

}
