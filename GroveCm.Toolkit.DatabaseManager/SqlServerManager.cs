using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace GroveCm.Toolkit.DatabaseManager
{
    public class SqlServerManager : IDisposable, IDatabaseManager
    {
        private SqlConnection _connection;

        public int Timeout { get; set; }

        public SqlServerManager(DatabaseConnectionConfig config, int timeout)
        {
            if (string.IsNullOrEmpty(config.UserName))
            {
                SetConnection(string.Format("Server={0}; Database={1}; Connect Timeout={2}; Integrated Security=true",
                    config.ServerName, config.DatabaseName, timeout));
            }
            else
            {
                SetConnection(string.Format("Server={0}; Database={1}; Connect Timeout={4}; User Id={2}; password={3}",
                    config.ServerName, config.DatabaseName, config.UserName, config.Password, timeout));
            }
        }

        public SqlServerManager(string connectionString)
        {
            SetConnection(connectionString);
        }

        private void SetConnection(string connectionString)
        {
            _connection = new SqlConnection(connectionString);
            _connection.Open();
        }

        public int RunNoDataSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null)
        {
            var command = new SqlCommand(sql, _connection) { CommandTimeout = Timeout };

            if (parameters != null)
            {
                foreach (SqlParameter parameter in parameters)
                {
                    command.Parameters.Add(parameter);
                }
            }

            return command.ExecuteNonQuery();
        }

        public DataTable RunDataTableSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null)
        {
            var da = new SqlDataAdapter();
            da.SelectCommand = new SqlCommand(sql, _connection) { CommandTimeout = Timeout };

            if (parameters != null)
            {
                foreach (SqlParameter parameter in parameters)
                {
                    da.SelectCommand.Parameters.Add(parameter);
                }
            }

            DataSet ds = new DataSet();

            da.Fill(ds);
            da.Dispose();
            return ds.Tables[0];
        }

        public DataSet RunDataSetSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null)
        {
            var da = new SqlDataAdapter();
            da.SelectCommand = new SqlCommand(sql, _connection) { CommandTimeout = Timeout };

            if (parameters != null)
            {
                foreach (SqlParameter parameter in parameters)
                {
                    da.SelectCommand.Parameters.Add(parameter);
                }
            }

            DataSet ds = new DataSet();

            da.Fill(ds);
            da.Dispose();
            return ds;
        }

        public IDataReader OpenReaderSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null)
        {
            var da = new SqlDataAdapter();
            da.SelectCommand = new SqlCommand(sql, _connection) { CommandTimeout = Timeout };

            if (parameters != null)
            {
                foreach (SqlParameter parameter in parameters)
                {
                    da.SelectCommand.Parameters.Add(parameter);
                }
            }
            da.SelectCommand.CommandTimeout = 0;

            return da.SelectCommand.ExecuteReader();
        }

        public void BulkInsert(string tableName, DataTable dataTable)
        {
            using (var bulkCopy = new SqlBulkCopy(_connection))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BulkCopyTimeout = Timeout;
                bulkCopy.WriteToServer(dataTable);
            }
        }

        public void Dispose()
        {
            _connection.Close();
            _connection.Dispose();
        }
    }
}
