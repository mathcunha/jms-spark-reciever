package receiver.jms.activemq;

import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import receiver.jms.JMSQueueReceiver;

public class JMSActiveMQReciever implements JMSQueueReceiver{

	private Boolean done = Boolean.FALSE;
	private Boolean error = Boolean.FALSE;
	private Logger logger = Logger.getLogger(getClass().getName());
	private Connection connection;
	private Session session;
	private Destination destination;
	private MessageConsumer consumer;
	private Consumer<String> store;
	private ExceptionListener listener;

	public static JMSActiveMQReciever getMessageReciever(ExceptionListener listener, Consumer<String> store,
			Map<String, String> properties) throws JMSException {
		return new JMSActiveMQReciever(listener, store, properties.get("brokerURL"), properties.get("queue"), properties.get("user"),
				properties.get("password"));
	}

	private JMSActiveMQReciever(ExceptionListener listener, Consumer<String> store, String brokerURL, String queue, String user, String password) throws JMSException {
		this.store = store;
		logger.info("Creating JMSActiveMQReciever for broker " + brokerURL + " and queue " + queue+" with credentias:"+user+":"+password);
		// Create a ConnectionFactory
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, brokerURL);
		try {
			connection = connectionFactory.createConnection();

			connection.start();
			connection.setExceptionListener(listener);

			// Create a Session
			session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			// Create the destination (Topic or Queue)
			destination = session.createQueue(queue);
			// Create a MessageConsumer from the Session to the Topic or Queue
			consumer = session.createConsumer(destination);

		} catch (JMSException e) {
			setDone();
			throw e;
		}
	}

	@Override
	public void run() {
		try {
			while (!done && !error) {

				logger.info("waiting message");
				Message message = consumer.receive();

				if (message instanceof TextMessage) {
					TextMessage textMessage = (TextMessage) message;
					String text = textMessage.getText();
					store.accept(text);
					logger.info("Received: " + text);
				} else {
					logger.info("Received: " + message);
				}
				if (message != null) {
					message.acknowledge();
				}
			}
		} catch (JMSException e) {
			listener.onException(e);
		}

	}

	@Override
	public void setDone() {
		logger.info("setDone for JMSActiveMQReciever");
		this.done = Boolean.TRUE;
		try {
			if (consumer != null) {
				consumer.close();
			}
		} catch (JMSException e) {
			setError("error closing consumer", e);
		}
		try {
			if (session != null) {
				session.close();
			}
		} catch (JMSException e1) {
			setError("error closing session", e1);
		}
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (JMSException e1) {
			setError("error closing connection", e1);
		}
	}

	public void setError(String message, Exception ex) {
		this.error = Boolean.TRUE;
		logger.log(Level.SEVERE, message, ex);
	}

	public Boolean hasError() {
		return this.error;
	}

}
