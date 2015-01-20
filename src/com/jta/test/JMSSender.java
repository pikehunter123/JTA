package com.jta.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.jms.JMSException;
import javax.sql.DataSource;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.atomikos.jms.AtomikosConnectionFactoryBean;
import com.atomikos.jms.extra.SingleThreadedJmsSenderTemplate;

public class JMSSender {

	private static String BROKER_URL = "tcp://localhost:"
			+ BrokerService.DEFAULT_PORT;

	// the globally unique resource name for the DB; change if needed
	private static String resourceName = "samples/jdbc/ExamplesJdbcPooledDatabase";

	// the data source, set by getDataSource
	private static AtomikosDataSourceBean ds = null;

	public static void main(String[] args) throws JMSException,
			NotSupportedException, SystemException, IllegalStateException,
			SecurityException, RollbackException, HeuristicMixedException,
			HeuristicRollbackException {

		String textMessage = "Max bet on red!";
		String qName = "TeleportQueue";

		// 1. prepare JMS
		// NOTE: you can also use the Atomikos QueueConnectionFactoryBean
		// to send messages, but then you have to create and manage connections
		// yourself.
		SingleThreadedJmsSenderTemplate session = null;
		// XXX : PLQ package change for activeMQ 4.1.2
		// create and configure an ActiveMQ factory
		ActiveMQXAConnectionFactory xaFactory = new ActiveMQXAConnectionFactory();
		xaFactory.setBrokerURL(BROKER_URL);

		// create a queue for ActiveMQ
		ActiveMQQueue queue = new ActiveMQQueue();
		queue.setPhysicalName(qName);

		// setup the Atomikos QueueConnectionFactory for JTA/JMS messaging
		AtomikosConnectionFactoryBean factory = new AtomikosConnectionFactoryBean();
		factory.setXaConnectionFactory(xaFactory);
		factory.setUniqueResourceName(qName + "Resource");

		// setup the Atomikos session for sending messages on
		session = new SingleThreadedJmsSenderTemplate();
		session.setAtomikosConnectionFactoryBean(factory);
		session.setDestination(queue);
		session.init();

		// 2. prepare JDBC
		boolean error = false;
		Connection conn = null;

		try {

			// Find or construct a datasource instance;
			// this could equally well be a JNDI lookup
			// where available. To keep it simple, this
			// demo merely constructs a new instance.
			ds = new AtomikosDataSourceBean();
			// REQUIRED: the full name of the XA datasource class

			ds.setXaDataSourceClassName("org.apache.derby.jdbc.EmbeddedXADataSource");
			Properties properties = new Properties();
			properties.put("databaseName", "db");
			properties.put("createDatabase", "create");
			ds.setXaProperties(properties);

			// REQUIRED: properties to set on the XA datasource class
			// ds.getXaProperties().setProperty("user", "demo");
			// REQUIRED: unique resource name for transaction recovery
			// configuration
			ds.setUniqueResourceName(resourceName);
			// OPTIONAL: what is the pool size?
			ds.setPoolSize(10);
			// OPTIONAL: how long until the pool thread checks liveness of
			// connections?
			ds.setBorrowConnectionTimeout(60);

			// NOTE: the resulting datasource can be bound in JNDI where
			// available

			conn = ds.getConnection();
			Statement s = conn.createStatement();
			try {
				s.executeUpdate("delete from Accounts");
			} catch (SQLException ex) {
				// table not there => create it
				s.executeUpdate("create table Accounts ("
						+ "account VARCHAR (20), " + "owner VARCHAR(300), "
						+ "balance DECIMAL (19,0) )");
			}
			s.close();

			// get a handle to the Atomikos transaction service
			UserTransaction utx = new UserTransactionImp();
			utx.setTransactionTimeout(60);

			// First, create a transaction
			utx.begin();

			session.sendTextMessage(textMessage);

			s = conn.createStatement();

			for (int i = 0; i < 100; i++) {
				s.executeUpdate("insert into Accounts values ( " + "'account"
						+ i + "' , 'owner" + i + "', 10000 )");
			}
			s.close();
		} catch (Exception e) {
			error = true;
			e.printStackTrace();
		} finally {

			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

			UserTransaction utx = new UserTransactionImp();
			if (utx.getStatus() != Status.STATUS_NO_TRANSACTION) {
				if (error) {
					utx.rollback();
					System.out.println("Do rollback!");
				} else {
					utx.commit();
					System.out.println("Transaction is succesfull");

					// when finished: close the sender session
					session.close();
				}
			} else
				System.out
						.println("WARNING: closeConnection called outside a tx");

		}

		// read table
		try {
			conn = ds.getConnection();
			Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery("select * from Accounts");

			if (rs.next()) {
				System.out.println("Reading table Accounts:");
				byte row = 1;
				do {
					System.out.printf(
							"row # %d: account=%s, owner=%s, balance=%s \n",
							row++, rs.getString(1), rs.getString(2),
							rs.getString(3));
				} while (rs.next());

			} else {
				System.out.println("Table Accounts is empty");
			}
			s.close();
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
