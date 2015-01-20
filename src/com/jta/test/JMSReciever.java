package com.jta.test;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;

import com.atomikos.jms.AtomikosConnectionFactoryBean;
import com.atomikos.jms.extra.MessageDrivenContainer;

public class JMSReciever {

	public static String BROKER_URL = "tcp://localhost:"
			+ BrokerService.DEFAULT_PORT;

	public static void main(String[] args) {
		try {
			
			System.out.println("Starting broker on "
					+ BrokerService.DEFAULT_PORT);

			BrokerService broker = new BrokerService();
			broker.setUseJmx(false);
			broker.addConnector(BROKER_URL);
			broker.start();
			
			while(true){
				
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
