import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

public class ManagerRunner {
    protected static Map<String, Client> clientsMap = new HashMap<>();
    protected static AtomicInteger jobIdsCounter = new AtomicInteger(100);
    protected static AtomicInteger taskIdsCounter = new AtomicInteger(100);
    protected static AtomicInteger numOfWorkers = new AtomicInteger(0);
    protected static Integer workersPerNTasks = 1;
    protected static AtomicInteger numOfTasks = new AtomicInteger(0);
    protected static Boolean terminate = false;
    private static EC2Toolkit ec2Toolkit = new EC2Toolkit();

    public static void main(String[] args) throws InterruptedException {
        SQSToolkit sqsToolkit = new SQSToolkit();
        S3Toolkit s3Toolkit = new S3Toolkit();
        String workersToManagerUrl = null;
        String managerToWorkersUrl = null;

        PropertiesReader prop = new PropertiesReader(System.getProperty("user.dir")+"/configure.properties");
        boolean setupSuccess = false;
        /* ---- SETUP ---- */
        while(!setupSuccess){
            try{
                s3Toolkit.CreateBucket(prop.getProperty("MANAGER_WORKERS_BUCKET"));

                /* ---- CREATE IN-OUT SQS FOR WORKERS ---- */
                String managerToWorkersStr = prop.getProperty("MANAGER_TO_WORKERS_QUEUE");
                managerToWorkersUrl = sqsToolkit.CreateSQS(managerToWorkersStr,"90");

                String workersToManagerStr = prop.getProperty("WORKERS_TO_MANAGER_QUEUE");
                workersToManagerUrl = sqsToolkit.CreateSQS(workersToManagerStr,"30");
                setupSuccess = true;
            } catch(Exception e){
                sleep(60*1000);
            }
        }



        List<Thread> locals = new LinkedList<>();
        List <Thread> workers= new LinkedList<>();
        for(int i = 0; i<7 ; i++) {
            Thread r = new Thread(new LocalReqHandler());
            locals.add(r);
            r.start();
        }
        for(int i = 0; i<3 ; i++) {
            Thread r = new Thread(new WorkerReqHandler());
            workers.add(r);
            r.start();
        }
        System.out.println( "wait for the threads of locals to finish == before join");
        for(Thread thread : locals) {
         thread.join();
        }
        System.out.println("wait for the workers take all the mission");
        // Waiting until all previous messages are handled by workers
        while (sqsToolkit.getNumOfMessages(managerToWorkersUrl) != 0);

        System.out.println("sending termination");
        while(getNumOfWorkers()>0 ){
            if(sqsToolkit.getNumOfMessages(managerToWorkersUrl) == 0) {
                sqsToolkit.SendMsgToQueue(managerToWorkersUrl, "terminate", " ");
            }
        }
        System.out.println("wait for the workers to exit\n");

        System.out.println("==========wait for the threads to finish==============");
        // wait for the threads that managers the workers to finish their run
        for(Thread thread : workers){
            System.out.printf("wait for the thread to join %d\n",thread.getId());
            thread.join();
            System.out.printf("the thread %d is join \n",thread.getId());
        }

        System.out.println("all workers thread are done now delete the queue");
        sqsToolkit.deleteQueue(workersToManagerUrl);
        sqsToolkit.deleteQueue(managerToWorkersUrl);
        sqsToolkit.deleteQueue(sqsToolkit.getQueueURL(prop.getProperty("LOCALS_TO_MANAGER_QUEUE_NAME")));
        // the manager kill himself
        ec2Toolkit.terminatedInstance(ec2Toolkit.getInstanceId());

    }

    public static synchronized void balanceNumOfWorkers(int change) {
        int numOfWorkers = getNumOfWorkers();
        int numOfTasks = ManagerRunner.numOfTasks.addAndGet(change);
        int desiredNumOfWorkers = (int) Math.ceil(numOfTasks / ManagerRunner.workersPerNTasks) - getNumOfWorkers();
//        int numOfWorkers = (int) Math.ceil(numOfTasks / ManagerRunner.workersPerNTasks) -ManagerRunner.numOfWorkers.get();
        if(desiredNumOfWorkers < 0)
            return;
        for (int i = 0; i < desiredNumOfWorkers && numOfWorkers <15; i++) {
            ec2Toolkit.CreateEC2Instance("WORKER_AMI");
            numOfWorkers++;
        }
        // check if everybody alive , if needed to upload new workers to replace the dead one
//        HandleRunningWorkers();

    }


    public static void deleteFile(String filename) {
        try {
            new File(filename).delete();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public static int getNumOfWorkers(){
        return ec2Toolkit.getAmountOfInstanceByTag("Worker");
    }
}

