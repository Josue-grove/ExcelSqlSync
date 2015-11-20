using System;
using GroveCm.ExcelSqlSync.Core;
using GroveCm.Toolkit.DatabaseManager;
using System.Configuration;

namespace OperationsConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            var appSettings = ConfigurationManager.AppSettings;
            var excelFile = args[0];
            var jsonConfig = args[1];

            var em = new ExcelManager();
            em.Open(excelFile);

            var workbookTables = em.ReadWorkbookTablesFromFile(jsonConfig);

            em.Update(workbookTables, new DatabaseConnectionConfig
            {
                ServerName = appSettings["SqlServerName"],
                DatabaseName = appSettings["DatabaseName"],
                UserName = Environment.GetEnvironmentVariable("DbUserName"),
                Password = Environment.GetEnvironmentVariable("DbUserPassword")
            });

            em.Save();

            Console.WriteLine("Done");
        }
    }
}
