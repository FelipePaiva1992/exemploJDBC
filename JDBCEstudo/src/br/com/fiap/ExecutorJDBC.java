package br.com.fiap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

public class ExecutorJDBC {

	private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/estudo";
	private static final String DB_USER = "root";
	private static final String DB_PASSWORD = "root";

	public static void main(String[] argv) throws SQLException {

		ExecutorJDBC executorJDBC = new ExecutorJDBC();
		executorJDBC.criacaoTabela();

		executorJDBC.jdbcTrancasao();
		executorJDBC.jdbcConcorrencia();
		executorJDBC.jdbcCache();

	}

	public void criacaoTabela() throws SQLException {
		Connection dbConnection = null;
		Statement statement = null;

		String createTableSQL = "CREATE TABLE `estudo`.`dbuser` (" + " `USER_ID` INT NOT NULL,"
				+ " `USERNAME` VARCHAR(20) NOT NULL," + " `CREATED_BY` VARCHAR(20) NOT NULL,"
				+ " `CREATED_DATE` DATETIME NOT NULL," + " PRIMARY KEY (`USER_ID`));";

		try {
			dbConnection = getDBConnection();
			statement = dbConnection.createStatement();

			System.out.println(createTableSQL);
			statement.execute(createTableSQL);

			System.out.println("Tabela \"dbuser\" is created!");

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		} finally {

			if (statement != null) {
				statement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}

	public void jdbcTrancasao() throws SQLException {

		Connection dbConnection = null;
		PreparedStatement preparedStatementInsert = null;
		PreparedStatement preparedStatementUpdate = null;

		String insertTableSQL = "INSERT INTO DBUSER" + "(USER_ID, USERNAME, CREATED_BY, CREATED_DATE) VALUES (?,?,?,?)";

		String updateTableSQL = "UPDATE DBUSER SET USERNAME =? " + "WHERE USER_ID = ?";

		try {
			dbConnection = getDBConnection();

			dbConnection.setAutoCommit(false);

			preparedStatementInsert = dbConnection.prepareStatement(insertTableSQL);
			preparedStatementInsert.setInt(1, 1);
			preparedStatementInsert.setString(2, "felipe");
			preparedStatementInsert.setString(3, "aplicacao");
			preparedStatementInsert.setTimestamp(4, getCurrentTimeStamp());
			preparedStatementInsert.executeUpdate();

			preparedStatementUpdate = dbConnection.prepareStatement(updateTableSQL);
			preparedStatementUpdate.setString(1, "felipe1");
			preparedStatementUpdate.setInt(2, 999);
			preparedStatementUpdate.executeUpdate();

			dbConnection.commit();

			System.out.println("Finalizado Transação!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			dbConnection.rollback();
		} finally {

			if (preparedStatementInsert != null) {
				preparedStatementInsert.close();
			}

			if (preparedStatementUpdate != null) {
				preparedStatementUpdate.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}
		}
	}

	public void jdbcConcorrencia() throws SQLException {

		Connection dbConnection = null;
		PreparedStatement preparedStatementInsert = null;

		String insertTableSQL = "INSERT INTO DBUSER" + "(USER_ID, USERNAME, CREATED_BY, CREATED_DATE) VALUES (?,?,?,?)";

		try {
			dbConnection = getDBConnection();
			dbConnection.setAutoCommit(false);

			preparedStatementInsert = dbConnection.prepareStatement(insertTableSQL, ResultSet.CONCUR_READ_ONLY);
			preparedStatementInsert.setInt(1, 2);
			preparedStatementInsert.setString(2, "felipe2");
			preparedStatementInsert.setString(3, "aplicacao");
			preparedStatementInsert.setTimestamp(4, getCurrentTimeStamp());
			preparedStatementInsert.executeUpdate();

			dbConnection.commit();

			System.out.println("Finalizado Concorrencia!");

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			dbConnection.rollback();

		} finally {

			if (preparedStatementInsert != null) {
				preparedStatementInsert.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}

	public void jdbcCache() throws SQLException {
		Connection dbConnection = null;
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			dbConnection = getDBConnection();
			dbConnection.setAutoCommit(false);
			ps = dbConnection.prepareStatement("SELECT * FROM estudo.dbuser");
			rs = ps.executeQuery();

			CachedRowSet crs = RowSetProvider.newFactory().createCachedRowSet();
			crs.populate(rs);

			ps.close();
			dbConnection.close();

			while (crs.next()) {
				crs.updateString("USERNAME", "Felipe Alterado");
				crs.updateRow();
			}
			crs.absolute(1);
			crs.deleteRow();

			dbConnection = getDBConnection();
			dbConnection.setAutoCommit(false);
			crs.acceptChanges(dbConnection);
			dbConnection.commit();

			crs.close();
			System.out.println("Finalizado Cache!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (rs != null) {
				rs.close();
			}

			if (ps != null) {
				ps.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}
		}
	}

	private static Connection getDBConnection() {

		Connection dbConnection = null;

		try {

			Class.forName(DB_DRIVER);

		} catch (ClassNotFoundException e) {

			System.out.println(e.getMessage());

		}

		try {

			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
			return dbConnection;

		} catch (SQLException e) {

			System.out.println(e.getMessage());

		}

		return dbConnection;

	}

	private static java.sql.Timestamp getCurrentTimeStamp() {

		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());

	}

}