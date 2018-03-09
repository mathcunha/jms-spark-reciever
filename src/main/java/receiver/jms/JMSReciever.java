package receiver.jms;

import java.util.Map;

import javax.jms.JMSException;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import receiver.jms.activemq.JMSActiveMQReciever;

public class JMSReciever extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, String> properties;
	private JMSQueueReceiver receiver;
	private JMSQueueFactory factory;

	private JMSReciever(StorageLevel storageLevel) {
		super(storageLevel);
	}

	public static JMSReciever buildJMSActiveMQReciever(Map<String, String> properties, StorageLevel storageLevel) {
		System.out.println(
				String.format("JMSReciever created with properties: brokerURL:%s, queue:%s, user:%s, password:%s, printMessages:%s",
						properties.get("brokerURL"), properties.get("queue"), properties.get("user"),
						properties.get("password"), properties.get("printMessages")));
		JMSReciever jms = new JMSReciever(storageLevel);
		jms.properties = properties;
		jms.factory = JMSActiveMQReciever::getMessageReciever;
		return jms;
	}

	@Override
	public void onStart() {
		try {
			receiver = factory.getJmsQueueReceiver(this::onException, super::store, properties);
			(new Thread(receiver::run)).start();
		} catch (JMSException e) {
			onException(e);
		}
	}

	@Override
	public void onStop() {
		if (receiver != null) {
			receiver.setDone();
			receiver = null;
		}
	}

	public synchronized void onException(JMSException ex) {
		super.restart(ex.getMessage(), ex);
	}
}
