package receiver.jms;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Consumer;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

@FunctionalInterface
public interface JMSQueueFactory extends Serializable{

	public JMSQueueReceiver getJmsQueueReceiver(ExceptionListener listener, Consumer<String> store,
			Map<String, String> properties) throws JMSException;
}
