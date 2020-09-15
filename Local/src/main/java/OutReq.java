import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.yield;

public class OutReq implements Runnable {
    private static AtomicInteger jobRequests = LocalRunner.jobRequests;
    private static String queueURL = LocalRunner.queueURL;
    private static String localRandId = LocalRunner.localRandId;
    private static PropertiesReader prop = LocalRunner.prop;
    SQSToolkit sqsToolkit = SQSToolkit.getToolkit();
    @Override
    public void run() {
        String input = null;
        try (Scanner scanner = new Scanner(System.in)) {
        while(input == null || !input.equals("2")) {
                if(input != null && !input.equals("1")){
                    System.out.println("I'm sorry, your selection is invalid");
                }
                System.out.println("Please select what you want to do next:");
                System.out.println("1) Add another Job");
                System.out.println("2) Terminate");

                while((input = scanner.nextLine()) == null);
                ;  // Read user input
                if(input.equals("1")){
                    parseAndSendNewJob(scanner);
                }
            }
            yield();
        } catch (Exception e){

        }
        System.out.println("Program will exit after all requested jobs are done.");
        yield();
    }
    private void parseAndSendNewJob(Scanner scanner) throws IOException {

        System.out.print("Enter new input filename  ");

        String inputName = scanner.nextLine();  // Read user input

        System.out.print("Enter new output filename: ");

        String outputName = scanner.nextLine();  // Read user input

        System.out.print("Enter number of PDFs per worker: ");

        String numOfWorkers = scanner.nextLine();  // Read user input

        Map<String, String> attrs = new HashMap<>();
        attrs.put("localQueueUrl",queueURL);
        attrs.put("bucketName",prop.getProperty("MANAGER_BUCKET_NAME"));
        attrs.put("filename",inputName);
        attrs.put("outputName",outputName);
        attrs.put("workersPerNTasks", numOfWorkers);
        sqsToolkit.SendMsgToQueue(prop.getProperty("LOCALS_TO_MANAGER_QUEUE_NAME"),
                "new task", attrs, localRandId);
        jobRequests.incrementAndGet();

    }
}
