// See https://aka.ms/new-console-template for more information
using DataFlowActions;

public class Program
{
    private static async Task Main(string[] args)
    {

        Console.WriteLine("Hello Data Flow");
        //var elr = new ETLProcess();
        var pipeline = new DataPipeline();
        var procssallandcolect = new CollectAllAndProcess();
        string[] databases = new string[] { "Connection str1", "Connection str2", "Connection str3" };
        //databases[0] = "";


        await pipeline.ExecutePipeline(databases);
        //prcoess all collected files 
        await procssallandcolect.ExecutePipeline();
        Console.ReadKey();
    }
}