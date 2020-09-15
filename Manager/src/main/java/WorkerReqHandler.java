import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.yield;

public class WorkerReqHandler implements Runnable {
    private SQSToolkit sqsToolkit = SQSToolkit.getToolkit();
    private S3Toolkit s3Toolkit = S3Toolkit.getToolkit();
    private EC2Toolkit ec2Toolkit = EC2Toolkit.getToolkit();
    private PropertiesReader prop = new PropertiesReader(System.getProperty("user.dir")+"/configure.properties");

    private String workersToManagerQueueUrl = sqsToolkit.getQueueURL(prop.getProperty("WORKERS_TO_MANAGER_QUEUE"));
    private static Map<String, Client> clientsMap = ManagerRunner.clientsMap;

    @Override
    public void run() {
        List<Message> messages = sqsToolkit.getQueueMessages(workersToManagerQueueUrl);
        while (0 < ManagerRunner.getNumOfWorkers()|| !ManagerRunner.terminate) {
            for (Message m : messages) {
                if (m.getBody().equals("terminate")) {
                    String instanceId = m.getMessageAttributes().get("senderId").getStringValue();
                    System.out.printf("got terminated msg from the worker %s\n",instanceId);
                    ec2Toolkit.terminatedInstance(instanceId);
                    System.out.printf("The curr num of workers is %d\n" , ManagerRunner.getNumOfWorkers());

                } else {
                    System.out.println("num of messages: " + messages.size());
                    handleCompletedTask(m);
                }
                sqsToolkit.deleteMsgFromQueue(workersToManagerQueueUrl, m);
            }
            messages = sqsToolkit.getQueueMessages(workersToManagerQueueUrl);
            yield();
        }
        System.out.printf("The thread %s is finish\n" , Thread.currentThread().getName());

    }


    private void handleCompletedTask(Message m) {
        Map<String, MessageAttributeValue> attrs = m.getMessageAttributes();
        String taskId = attrs.get("taskId").getStringValue();
        String res = attrs.get("res").getStringValue();
        String success = attrs.get("success").getStringValue();
        String clientId = attrs.get("senderId").getStringValue();
        String jobId = attrs.get("jobId").getStringValue();

        String bucketName = prop.getProperty("MANAGER_WORKERS_BUCKET");
        res = Boolean.parseBoolean(success) ? s3Toolkit.generateUrl(bucketName, taskId) : res;
        Client c = clientsMap.get(clientId);
        Job j = c.getJobsMap().get(jobId);
        int tasksLeft = c.markTaskCompleted(jobId, taskId, success, res);
        System.out.printf("Completed task %s, tasks left: %d\n", taskId, tasksLeft);
        //TODO: clean the jobs queue for every job that has no tasks left
        if (tasksLeft == 0 && j.getAllJobReceived()) {
            System.out.printf("Sending result to client %s.\n", c.getId());
            String localQueueUrl = c.getQueueUrl();
            generateSummaryFile(j);
            bucketName = prop.getProperty("MANAGER_BUCKET_NAME");
            Map <String,String> attr = new HashMap<>();
            attr.put("url", s3Toolkit.generateUrl(bucketName, j.getOutputName()));
            attr.put("outputName",j.getOutputName());
            s3Toolkit.UploadFile(bucketName,
                    j.getOutputName() + ".html",
                    j.getOutputName());
            sqsToolkit.SendMsgToQueue(localQueueUrl,
                    "done task",
                    attr,
                    clientId);
            c.getJobsMap().remove(jobId);
            ManagerRunner.deleteFile(j.getOutputName());
            // if we finished all the client jobs, we cant delete this client.
            if(c.getJobsMap().isEmpty()) {
                clientsMap.remove(clientId, c);
            }
        }

        sqsToolkit.deleteMsgFromQueue(workersToManagerQueueUrl, m);
    }

    private void generateSummaryFile(Job j) {
        String html = "<html><body>";
        for (Map.Entry<String, Task> tEntry : j.getTasksMap().entrySet()) {
            Task t = tEntry.getValue();
            html += t.toString() + "<br>";
        }
        html += "</body></html>";
        try {
            PrintWriter out = new PrintWriter(j.getOutputName() + ".html");
            out.println(html);
            out.close();
        } catch (Exception e) {

        }
    }
}
