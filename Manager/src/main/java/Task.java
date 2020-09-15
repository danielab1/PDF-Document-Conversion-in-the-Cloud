
public class Task {
    private TASK_STATUS status;
    private String taskId;
    private String description;
    private String inputUrl;
    private String cmd;

    public Task(String taskId, String inputUrl, String cmd) {
        this.taskId = taskId;
        this.inputUrl = inputUrl;
        this.status = TASK_STATUS.PENDING;
        this.cmd = cmd;
    }

    public TASK_STATUS getStatus() {
        return status;
    }

    public void setStatus(TASK_STATUS status) {
        this.status = status;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getInputUrl() {
        return inputUrl;
    }

    public void setInputUrl(String inputUrl) {
        this.inputUrl = inputUrl;
    }

    public String toString(){
        return cmd+"\t"+inputUrl+"\t"+description;

    }
}
