using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace DurableFunctionsDemo
{
    public static class Calculator
    {
        [Function(nameof(Calculator))]
        public static async Task<List<int>> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(Calculator));
            logger.LogInformation("Starting orchestration.");

            var tasks = new List<Task<int>>();

            // Define the list of number pairs to multiply
            var numberPairs = new List<int[]>
            {
                new int[] { 6, 7 },
                new int[] { 8, 9 },
                new int[] { 10, 11 }
            };

            // Call the SubOrchestrator with the number pairs
            int[] totalResult = await context.CallSubOrchestratorAsync<int[]>(nameof(MultiplySubOrchestrator), numberPairs);

            logger.LogInformation("Total result from sub-orchestrator: {totalResult}", totalResult);
            return totalResult.ToList();
        }

        //This function simulates top-level parameter calculation
        //It is capable of envoking any number of dependent parameter calculations (i.e. "MultiplyNumber") in parallel
         [Function(nameof(MultiplySubOrchestrator))]
        public static async Task<int[]> MultiplySubOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context, int[][] numberPairs)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(MultiplySubOrchestrator));
            logger.LogInformation("Starting multiply sub-orchestrator.");

            var tasks = new List<Task<int>>();

            // Schedule multiple concurrent MultiplyNumbers tasks (i.e. fan-out pattern)
            foreach (var pair in numberPairs)
            {
                tasks.Add(context.CallActivityAsync<int>("MultiplyNumbers", pair));
            }

            // Wait for all concurrent tasks to complete (i.e. fan-in pattern)
            var results = await Task.WhenAll(tasks);

           

            logger.LogInformation("Total result of all multiplications in sub-orchestrator: {totalResult}", results);
            return results;
        }

        //This function simulates the dependent parameter calculation
        //Multiples of this function can be run in parallel in order to bring the dependent results up to the 
        //top-level parameter calculation
        [Function("MultiplyNumbers")]
        public static int Multiply([ActivityTrigger] int[] numbers, FunctionContext context)
        {
            ILogger logger = context.GetLogger("MultiplyNumbers");
            logger.LogInformation("Multiplying numbers: {number1} and {number2}", numbers[0], numbers[1]);

            int result = numbers[0] * numbers[1];

            logger.LogInformation("Result of multiplication: {result}", result);
            return result;
        }

        [Function("HelloOrchestration_HttpStart")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("HelloOrchestration_HttpStart");

            // Start a new orchestration instance
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(nameof(Calculator));
            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);
            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }
    }
}
