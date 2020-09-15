import com.amazonaws.services.sqs.model.Message;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkerRunner {
    public static void main(String[] args) throws Exception {
        SQSToolkit sqsToolkit = SQSToolkit.getToolkit();
        S3Toolkit s3Toolkit = S3Toolkit.getToolkit();
        EC2Toolkit ec2Toolkit = EC2Toolkit.getToolkit();
        PropertiesReader prop = new PropertiesReader(System.getProperty("user.dir")+"/configure.properties");

        String inUrl = sqsToolkit.getQueueURL(prop.getProperty("MANAGER_TO_WORKERS_QUEUE"));
        String outUrl = sqsToolkit.getQueueURL(prop.getProperty("WORKERS_TO_MANAGER_QUEUE"));
        s3Toolkit.CreateBucket(prop.getProperty("MANAGER_WORKERS_BUCKET"));

        List<Message> Msgs = sqsToolkit.getQueueMessages(prop.getProperty("MANAGER_TO_WORKERS_QUEUE"));
        boolean terminate = false;
        while (!terminate) {
            for (Message msg : Msgs) {
                if (msg.getBody().equals("terminate") && !terminate) {
                    sqsToolkit.deleteMsgFromQueue(inUrl,msg);
                    terminate = true;
                } else if(msg.getBody().equals("new PDF task")) {
                    String cmd = msg.getMessageAttributes().get("cmd").getStringValue();
                    String url = msg.getMessageAttributes().get("url").getStringValue();
                    String taskId = msg.getMessageAttributes().get("taskId").getStringValue();
                    String senderId = msg.getMessageAttributes().get("senderId").getStringValue();
                    String jobId = msg.getMessageAttributes().get("jobId").getStringValue();
                    System.out.printf("Working on task: %s to %s\n", taskId, cmd);
                    String res = taskId;

                    Boolean success = DownloadFile(url, taskId);
                    System.out.printf("file was downloaded successfully\n for task id %s", taskId);
                    if (!success) {
                        res = "Download failed";
                        System.out.println(res);
                    }

                    success = ConvertFile(cmd, taskId);
                    if (!success) {
                        res = "Conversion failed";
                        System.out.println(res);
                    }

                    if (success) {
                        s3Toolkit.UploadFile(prop.getProperty("MANAGER_WORKERS_BUCKET"),
                                createFileName(cmd, taskId),
                                taskId);
                    }

                    cleanFiles(taskId, cmd);

                    Map<String, String> attrs = new HashMap<>();
                    attrs.put("taskId", taskId);
                    attrs.put("res", res);
                    attrs.put("success", success.toString());
                    attrs.put("cmd", cmd);
                    attrs.put("filename", createFileName(cmd, taskId));
                    attrs.put("jobId", jobId);
                    sqsToolkit.SendMsgToQueue(outUrl, "done PDF task", attrs, senderId);
                    sqsToolkit.deleteMsgFromQueue(inUrl, msg);

                }
            }
            if (!terminate)
                Msgs = sqsToolkit.getQueueMessages(prop.getProperty("MANAGER_TO_WORKERS_QUEUE"));
            else sqsToolkit.SendMsgToQueue(outUrl, "terminate", ec2Toolkit.getInstanceId());
        }

    }


    private static void cleanFiles(String taskId, String cmd) {
        try {
            (new File(createFileName("", taskId))).delete();
            (new File(createFileName(cmd, taskId))).delete();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public static boolean DownloadFile(String urlStr, String filename) throws MalformedURLException {
        System.out.printf("Downloading %s\n", urlStr);
        URL url = new URL(urlStr);
        try {
            HttpURLConnection conn;
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            conn.connect();
            InputStream in = conn.getInputStream();
            Files.copy(in, Paths.get(filename + ".pdf"), StandardCopyOption.REPLACE_EXISTING);
            in.close();
        } catch (IOException e) {
            // handle exception
            return false;
        }
        return true;
    }

    public static String createFileName(String cmd, String taskId) {
        if (cmd.equals("ToImage")) {
            return taskId + ".jpg";
        } else if (cmd.equals("ToText")) {
            return taskId + ".txt";
        } else if (cmd.equals("ToHTML")) {
            return taskId + ".html";
        } else {
            return taskId + ".pdf";
        }
    }

    public static boolean ConvertFile(String cmd, String taskId) throws Exception {
        boolean convert = true;
        if (cmd.equals("ToImage")) {
            convert = PDFtoJPG(taskId);
        } else if (cmd.equals("ToText")) {
            convert = PDFtoText(taskId);
        } else if (cmd.equals("ToHTML")) {
            convert = PDFtoHTML(taskId);
        }
        return convert;
    }

    public static boolean PDFtoJPG(String filename) throws Exception {
        System.out.printf("converting %s to image\n", filename);
        try {
            PDDocument pd = PDDocument.load(new File(filename + ".pdf"));
            PDFRenderer pr = new PDFRenderer(pd);
            BufferedImage bi = pr.renderImageWithDPI(0, 300);
            ImageIO.write(bi, "JPEG", new File(filename + ".jpg"));
            pd.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean PDFtoHTML(String filename) throws IOException {
        System.out.printf("converting %s to HTML\n", filename);
        try {
            PDDocument doc = PDDocument.load(new File(filename + ".pdf"));
            PDFText2HTML stripper = new PDFText2HTML();
            String content = stripper.getText(doc);
            PrintWriter out = new PrintWriter(filename + ".html");
            out.println(content);
            out.close();
            doc.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean PDFtoText(String filename) throws IOException {
        System.out.printf("converting %s to text", filename);
        try {
            PDDocument doc = PDDocument.load(new File(filename + ".pdf"));
            String content = new PDFTextStripper().getText(doc);

            PrintWriter out = new PrintWriter(filename + ".txt");
            out.println(content);
            out.close();
            doc.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
