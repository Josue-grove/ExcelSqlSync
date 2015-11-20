using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace GroveCm.Toolkit.DatabaseManager
{
    public interface IDatabaseManager
    {
        int Timeout { get; set; }
        int RunNoDataSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null);
        DataTable RunDataTableSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null);
        DataSet RunDataSetSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null);
        IDataReader OpenReaderSqlCommand(string sql, IEnumerable<SqlParameter> parameters = null);
        void BulkInsert(string tableName, DataTable dataTable);
        void Dispose();
    }
}
