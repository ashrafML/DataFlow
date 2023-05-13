using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Data;
using System.Text;

namespace DataFlowActions
{


    public class DataPipeline
    {
        private TransformBlock<string, List<Dictionary<string, object>>> extractBlock;
        private TransformBlock<List<Dictionary<string, object>>, DataTable> removeColumnsBlock;
        private ActionBlock<DataTable> saveBlock;

        public DataPipeline()
        {
            var executionOptions = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount // Adjust as needed
            };

            extractBlock = new TransformBlock<string, List<Dictionary<string, object>>>(ExtractDataAsync, executionOptions);
            removeColumnsBlock = new TransformBlock<List<Dictionary<string, object>>, DataTable>(RemoveColumns, executionOptions);
            saveBlock = new ActionBlock<DataTable>(SaveDataAsync, executionOptions);

            extractBlock.LinkTo(removeColumnsBlock);
            removeColumnsBlock.LinkTo(saveBlock);
        }

        public async Task ExecutePipeline(string[] databases)
        {
            foreach (var database in databases)
            {
                await extractBlock.SendAsync(database);
            }

            extractBlock.Complete();
            await saveBlock.Completion;
        }

        private async Task<List<Dictionary<string, object>>> ExtractDataAsync(string connectionString)
        {
            var data = new List<Dictionary<string, object>>();
            Console.WriteLine("Start Read......");
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlCommand command = new SqlCommand("SELECT * FROM YourTable", connection))
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess))
                    {
                        var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();

                        while (await reader.ReadAsync())
                        {
                            var row = new Dictionary<string, object>();
                            foreach (var columnName in columnNames)
                            {
                                row[columnName] = await reader.GetFieldValueAsync<object>(reader.GetOrdinal(columnName));
                            }
                            data.Add(row);
                        }
                    }
                }
            }

            return data;
        }

        private DataTable RemoveColumns(List<Dictionary<string, object>> data)
        {
            var dataTable = new DataTable();
            Console.WriteLine("Start Remove Column......");
            var columnNames = data.SelectMany(row => row.Keys).Distinct();

            foreach (var columnName in columnNames)
            {
                dataTable.Columns.Add(columnName);
            }

            foreach (var row in data)
            {
                var dataRow = dataTable.NewRow();
                foreach (var column in row)
                {
                    dataRow[column.Key] = column.Value;
                }
                dataTable.Rows.Add(dataRow);
            }

            return dataTable;
        }

        private async Task SaveDataAsync(DataTable dataTable)
        {
            Console.WriteLine("Start Save......");
            using (StreamWriter file = new StreamWriter(@"output.csv", true))
            {
                foreach (DataRow row in dataTable.Rows)
                {
                    var values = row.ItemArray.Select(x => x.ToString());
                    await file.WriteLineAsync(string.Join(",", values));
                }
            }
            Console.WriteLine("Saved!!");
        }
    }

   
}
