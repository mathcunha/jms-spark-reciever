package receiver.jms;

public interface JMSQueueReceiver {
	public void run();

	public void setDone();
}
