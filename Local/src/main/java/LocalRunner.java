import com.amazonaws.services.sqs.model.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalRunner {
    protected static AtomicInteger jobRequests = new AtomicInteger(1);
    protected static String queueURL = null;
    protected static String localRandId = null;
    protected static PropertiesReader prop = null;
    public static void main(String[] args) throws IOException {
        /* ---- SETTING UP NECESSARY PREREQUISITES ---- */
        EC2Toolkit ec2Toolkit = EC2Toolkit.getToolkit();
        SQSToolkit sqsToolkit = SQSToolkit.getToolkit();
        S3Toolkit s3Toolkit = S3Toolkit.getToolkit();
        prop = new PropertiesReader(System.getProperty("user.dir")+"/configure.properties");

        // Setting up unique Identifier
        localRandId = ConvertIpToString(InetAddress.getLocalHost().getHostAddress());
        String queueName = "local_to_manager_" + localRandId;
        queueURL = sqsToolkit.CreateSQS(queueName,"30");

        //Upload manager instance if not running
        if(!ec2Toolkit.isInstanceRunning("Manager")){
            ec2Toolkit.CreateEC2Instance("MANAGER_AMI");
        }

        //Open needed sqs for interaction between local to manager
        String localsToManagerQueueName = prop.getProperty("LOCALS_TO_MANAGER_QUEUE_NAME");
        sqsToolkit.CreateSQS(localsToManagerQueueName,"30");
        String localsToManagerQueueURL = sqsToolkit.getQueueURL(localsToManagerQueueName);

        /* ---- SETTING UP INPUT FILE FOR UPLOAD ---- */
        String bucketName = prop.getProperty("MANAGER_BUCKET_NAME");
        String inputName = args[0];
        String outputName = args[1];
//        String userDirectory = new File("").getAbsolutePath();
        String inputPath = new File("").getAbsolutePath()+"/"+inputName+".txt";
        try{
            s3Toolkit.CreateBucket(bucketName);
            s3Toolkit.UploadFile(bucketName,
                    inputPath,
                    inputName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        /* ---- SENDING FIRST JOB ---- */
        Map<String, String> attrs = new HashMap<>();
        attrs.put("localQueueUrl",queueURL);
        attrs.put("bucketName",bucketName);
        attrs.put("filename",inputName);
        attrs.put("outputName",outputName);
        attrs.put("workersPerNTasks", args[2]);
        sqsToolkit.SendMsgToQueue(localsToManagerQueueURL, "new task", attrs, localRandId);



        OutReq outReq = new OutReq();
        Thread t1 = new Thread(outReq);
        t1.start();
        List<Message> msgs = sqsToolkit.getQueueMessages(queueURL);
        StringBuilder html = new StringBuilder();
        while(0<jobRequests.intValue()){
            for(Message m :msgs){
                if(m.getBody().equals("done task")){
                    outputName = m.getMessageAttributes().get("outputName").getStringValue();
                    s3Toolkit.DownloadFile(bucketName,outputName, line -> html.append(line));
                    PrintWriter out = null;
                    try {
                        out = new PrintWriter(outputName + ".html");
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    out.println(html.toString());
                    out.close();
                    sqsToolkit.deleteMsgFromQueue(queueURL,m);
                    jobRequests.decrementAndGet();
                }
            }
            msgs = sqsToolkit.getQueueMessages(queueURL);
        }

        try {
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(4 <= args.length && args[3].equals("terminate"))
            sqsToolkit.SendMsgToQueue(localsToManagerQueueURL, "terminate", attrs, localRandId);
        sqsToolkit.deleteQueue(queueURL);

    }

    public static String ConvertIpToString(String ip){
        StringBuilder s = new StringBuilder(ip);
        for(int i = 0; i< s.length(); i++){
            if(s.charAt(i) != '.'){
                s.replace(i,i+1, String.valueOf((char)(s.charAt(i)+'a'-'0')));
            }
            else s.replace(i,i+1, "_");
        }
        return s.toString();
    }
}
