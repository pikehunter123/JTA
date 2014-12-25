package com.jta.test;

import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.XAConnection;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import com.atomikos.icatch.jta.UserTransactionManager;

public class JDBCSender {

	private static XADataSource xadsOne = null;
	private static XADataSource xadsTwo = null;

	public static void main(String[] args) {

		boolean error = false;

		XAConnection xaconnOne = null;
		XAConnection xaconnTwo = null;

		Transaction tx = null;

		XAResource xaresJDBCOne = null;
		XAResource xaresJDBCTwo = null;

		TransactionManager tm = null;

		Connection connOne = null;
		Connection connTwo = null;

		try {
			// Инициализация DataSource
			initXADataSourceOne();
			initXADataSourceTwo();

			xaconnOne = xadsOne.getXAConnection();
			connOne = xaconnOne.getConnection();
			// connOne.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			// connOne.setAutoCommit(false);

			xaconnTwo = xadsTwo.getXAConnection();
			connTwo = xaconnTwo.getConnection();
			// connTwo.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			// connTwo.setAutoCommit(false);

			// Подготовка таблицы
			checkTable(connOne);
			clearTable(connOne);

			checkTable(connTwo);
			clearTable(connTwo);

			// Глобальная транзакция

			tm = new UserTransactionManager();
			tm.setTransactionTimeout(60);
			// First, create a transaction
			tm.begin();



			xaresJDBCOne = xaconnOne.getXAResource();
			xaresJDBCTwo = xaconnTwo.getXAResource();

			// get the current tx
			tx = tm.getTransaction();
			// enlist; if this is the first time the
			// resource is used then this will also trigger
			// recovery
			tx.enlistResource(xaresJDBCOne);
			tx.enlistResource(xaresJDBCTwo);

			// Операции
			String msg = "The bomb has been planted!";

			// Вставка в БД Derby
			Statement st = connOne.createStatement();
			st.executeUpdate("insert into BOMBS(MSG) VALUES('" + msg + "')");
			st.close();

			tm.begin();
			
			st = connTwo.createStatement();
			st.executeUpdate("insert into BOMBS(MSG) VALUES('" + msg + "')");
			st.close();

			readTable(connOne);
			readTable(connTwo);

			throw new Exception();

		} catch (IllegalStateException er) {
			error = true;
			er.printStackTrace();
		} catch (Exception e) {
			error = true;
			e.printStackTrace();
		}

		finally {
			if (xaconnOne != null && xaconnTwo != null) {
				try {
					int flag = XAResource.TMSUCCESS;
					// closeConnection
					if (error)
						flag = XAResource.TMFAIL;

					tx.delistResource(xaresJDBCOne, flag);
					tx.delistResource(xaresJDBCTwo, flag);
					// close the JDBC user connection
					connOne.close();
					connTwo.close();

					if (error) {
						System.out.println("Ошибка. Делаем откат!");
						tm.rollback();
					} else {
						System.out.println("Успешная передача!");
						tm.commit();
					}
					// close XAConnection AFTER commit, or commit will fail!
					xaconnOne.close();
					xaconnTwo.close();

					Connection con = xadsOne.getXAConnection().getConnection();
					readTable(con);
					con.close();

					con = xadsTwo.getXAConnection().getConnection();
					readTable(con);
					con.close();

				} catch (SQLException e) {
					e.printStackTrace();
				} catch (IllegalStateException e) {
					e.printStackTrace();
				} catch (SystemException e) {
					e.printStackTrace();
				} catch (SecurityException e) {
					e.printStackTrace();
				} catch (HeuristicMixedException e) {
					e.printStackTrace();
				} catch (HeuristicRollbackException e) {
					e.printStackTrace();
				} catch (RollbackException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void clearTable(Connection conn) {
		try {
			Statement st = conn.createStatement();
			st.executeUpdate("delete from BOMBS");
			st.close();
			System.out.println("Таблица BOMBS очищена");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void readTable(Connection conn) {
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery("select MSG from BOMBS");
			byte row = 0;
			System.out.println("Содержимое таблицы BOMBS:");
			while (rs.next()) {
				row++;
				System.out.println("\tRow " + row + ", Field MSG: " + rs.getString(1));
			}
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void checkTable(Connection conn) {
		try {
			Statement st = conn.createStatement();
			st.executeUpdate("create table BOMBS (MSG VARCHAR(100))");
			st.close();
			System.out.println("Таблица BOMBS создана");
		} catch (SQLException e) {
			System.out.println("Таблица BOMBS уже существует!");
		}
	}

	public static void initXADataSourceOne() throws Exception {
		// retrieve or construct a third-party XADataSource
		org.apache.derby.jdbc.EmbeddedXADataSource ds = new org.apache.derby.jdbc.EmbeddedXADataSource();
		ds.setDatabaseName("dbOne");
		ds.setCreateDatabase("create");
		xadsOne = ds;
	}

	public static void initXADataSourceTwo() throws Exception {
		// retrieve or construct a third-party XADataSource
		org.apache.derby.jdbc.EmbeddedXADataSource ds = new org.apache.derby.jdbc.EmbeddedXADataSource();
		ds.setDatabaseName("dbTwo");
		ds.setCreateDatabase("create");
		xadsTwo = ds;
	}

}
