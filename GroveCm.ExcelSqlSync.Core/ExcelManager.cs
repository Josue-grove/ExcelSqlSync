using OfficeOpenXml;
using System.IO;
using System.Collections.Generic;
using GroveCm.Toolkit.DatabaseManager;
using System;
using Newtonsoft.Json;

namespace GroveCm.ExcelSqlSync.Core
{
    public class ExcelManager : IDisposable
    {
        public ExcelPackage ExcelPackage { get; set; }
        public ExcelWorksheets Worksheets
        {
            get
            {
                return ExcelPackage.Workbook.Worksheets;
            }
        }

        public void Open(string fileName)
        {
            var f = new FileInfo(fileName);
            ExcelPackage = new ExcelPackage(f);
        }

        public void Save()
        {
            ExcelPackage.Save();
        }

        public void Update(Dictionary<string, string> workbookTables, DatabaseConnectionConfig dbConnConfig)
        {
            IDatabaseManager dbm = null;
            try
            {
                dbm = new SqlServerManager(dbConnConfig, 0);
                foreach (var workbookTable in workbookTables)
                {
                    Console.WriteLine($"Updating {workbookTable.Key}");
                    var worksheet = Worksheets[workbookTable.Key];
                    if (worksheet == null)
                    {
                        worksheet = Worksheets.Add(workbookTable.Key);
                    }
                    else
                    {
                        worksheet.DeleteColumn(1, worksheet.Dimension.Columns);
                    }

                    var reader = dbm.OpenReaderSqlCommand($"select * from [adhoc].[{StopSqlInjection(workbookTable.Value)}]");

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        worksheet.Cells[1, i + 1].Value = reader.GetName(i);
                    }

                    var sheetRow = 2;
                    foreach (var row in reader.Rows())
                    {
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            worksheet.Cells[sheetRow, i + 1].Value = reader[i].ConvertTo<string>();
                        }
                        sheetRow++;
                    }

                    reader.Close();
                    reader.Dispose();
                }
            }
            finally
            {
                if (dbm != null) { dbm.Dispose(); }
            }
        }

        static string StopSqlInjection(string value)
        {
            return value.Replace("]", "").Replace(";", "").Replace("\n", "");
        }

        public Dictionary<string, string> ReadWorkbookTablesFromFile(string fileName)
        {
            var json = File.ReadAllText(fileName);
            return JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
        }

        public void Dispose()
        {
            ExcelPackage.Dispose();
        }
    }
}
