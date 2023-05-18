using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace DataFlowActions
{
    internal class CollectAllAndProcess
    {
        TransformBlock<string, Tuple<string[], string[][]>> loadCsvBlock;
        List<string> processedData = new List<string>();
        ActionBlock<string[]> addToProcessedDataBlock;
        public CollectAllAndProcess() {
         

            // Define the dataflow blocks.
             loadCsvBlock = new TransformBlock<string, Tuple<string[], string[][]>>(loadCsv);
            //remove all columns execpt text and reslut
            var filterColumnsBlock = new TransformBlock<Tuple<string[], string[][]>, string[]>(filterColumns);
            //remove all white spaces 
            var removeSpacesBlock = new TransformBlock<string[], string[]>(removeSpaces);
            //process text and remove numbers
            var removeSubsAndNumbersBlock = new TransformBlock<string[], string[]>(removeSubsAndNumbers);
            //save processed to list
             addToProcessedDataBlock = new ActionBlock<string[]>(data =>
            {
                // Add the processed data to the list.
                processedData.AddRange(data);
            });

            // Link the blocks together.
            loadCsvBlock.LinkTo(filterColumnsBlock, new DataflowLinkOptions { PropagateCompletion = true });
            filterColumnsBlock.LinkTo(removeSpacesBlock, new DataflowLinkOptions { PropagateCompletion = true });
            removeSpacesBlock.LinkTo(removeSubsAndNumbersBlock, new DataflowLinkOptions { PropagateCompletion = true });
            removeSubsAndNumbersBlock.LinkTo(addToProcessedDataBlock, new DataflowLinkOptions { PropagateCompletion = true });

            // Define a list of directories where your files are located.
            var directories = new List<string>
        {
            @"C:\path\to\your\csv\files1",  // modify these paths to your csv files
            @"C:\path\to\your\csv\files2",
            // Add more paths as needed.
        };

            // Process each file in each directory.
            foreach (var directory in directories)
            {
                foreach (var path in Directory.GetFiles(directory, "*.csv"))
                {
                    loadCsvBlock.Post(path);
                }
            }

            // Mark the head of the pipeline as complete.
          
        }
        public async Task ExecutePipeline()
        {
            loadCsvBlock.Complete();

            // Wait for the last block in the pipeline to process all messages.
            await addToProcessedDataBlock.Completion;

            // Now that all files are processed, save the processed data as one output file.
            var outputPath = @"C:\path\to\your\output\file.csv"; // modify
            await File.WriteAllLinesAsync(outputPath, processedData);
        }
        private Tuple<string[], string[][]> loadCsv(string path)
        {
            var lines = new List<string[]>();
            string[] headers = null;
            using (var parser = new TextFieldParser(path))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                while (!parser.EndOfData)
                {
                    var fields = parser.ReadFields();
                    if (headers == null)
                    {
                        headers = fields; // read header line
                    }
                    else
                    {
                        lines.Add(fields);
                    }
                }
            }
        
            return new Tuple<string[], string[][]>(headers, lines.ToArray());

        }

        private string[] filterColumns(Tuple<string[], string[][]> data)
        {
            var headers = data.Item1;
            var lines = data.Item2;
            var result = new List<string>();
            var textIndex = Array.IndexOf(headers, "text");
            var sentimentIndex = Array.IndexOf(headers, "sentiment");

            foreach (var line in lines)
            {
                result.Add(string.Join(",", line[textIndex], line[sentimentIndex]));
            }
            return result.ToArray();
        }

        private string[] removeSpaces(string[] data)
        {
            return Array.ConvertAll(data, s => s.Trim());
        }
        private string[] removeSubsAndNumbers(string[] data)
        {
            return Array.ConvertAll(data, s => Regex.Replace(Regex.Replace(s, @"[\d-]", string.Empty), @"[^a-zA-Z\s,]", string.Empty));
        }

    }
}
