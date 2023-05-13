using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks.Dataflow;
using CsvHelper;

namespace DataFlowActions
{

    public class ETLProcess
    {
        private TransformManyBlock<string[], DataTable> extractBlock;
        private TransformBlock<DataTable, DataTable> transformBlock;
        private ActionBlock<DataTable> loadBlock;

        public ETLProcess()
        {
            // Define the blocks
            extractBlock = new TransformManyBlock<string[], DataTable>(connStrs => connStrs.Select(connStr => Extract(connStr)));
            transformBlock = new TransformBlock<DataTable, DataTable>(data => Transform(data));
            loadBlock = new ActionBlock<DataTable>(data => Load(data));

            // Link the blocks to form a pipeline
            extractBlock.LinkTo(transformBlock, new DataflowLinkOptions { PropagateCompletion = true });
            transformBlock.LinkTo(loadBlock, new DataflowLinkOptions { PropagateCompletion = true });
        }

        public void Start(string[] connectionStrings)
        {
            // Start the ETL process
            extractBlock.Post(connectionStrings);
            extractBlock.Complete();
            loadBlock.Completion.Wait();
        }

        private DataTable Extract(string connectionString)
        {
            // Extract data from the database
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlDataAdapter adapter = new SqlDataAdapter("SELECT *  FROM YourTable", connection);
                DataTable dt = new DataTable();
                adapter.Fill(dt);
                return dt;
            }
        }

        private DataTable Transform(DataTable table)
        {
            // Transform data
            // For this example,remove field null
            Console.Write("data count"+ table.Rows.Count.ToString()+"/n");
            foreach (var column in table.Columns.Cast<DataColumn>().ToArray())
            {
                if (table.AsEnumerable().All(dr => dr.IsNull(column)))
                    table.Columns.Remove(column);
            }
            return table;
        }

        private void Load(DataTable data)
        {
            Console.WriteLine("Begin save /n");
            StringBuilder sb = new StringBuilder();

            string[] columnNames = data.Columns.Cast<DataColumn>().
                                  Select(column => column.ColumnName).
                                  ToArray();
            sb.AppendLine(string.Join(",", columnNames));

            foreach (DataRow row in data.Rows)
            {
                string[] fields = row.ItemArray.Select(field => field.ToString()).
                                                ToArray();
                sb.AppendLine(string.Join(",", fields));
            }

            File.WriteAllText("test.csv", sb.ToString());
        
            Console.WriteLine("Data Saved successfully");
        }
    }

}
