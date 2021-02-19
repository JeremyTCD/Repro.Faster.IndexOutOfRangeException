using FASTER.core;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Repro.Faster.IndexOutOfRangeException
{
    public class Program
    {
        public static async Task Main()
        {
            var mixedStorageDictionary = new MixedStorageDictionary<int, string>();
            int numRecords = 20_000_000;

            Parallel.For(0, numRecords, key => mixedStorageDictionary.Upsert(key, "dummyString"));

            Console.WriteLine("upsert done");

            int issueParallel = 4; // vary this to increase read issue parallelism
            Task[] tasks = new Task[issueParallel];
            Random[] r = new Random[issueParallel];
            for (int i = 0; i < issueParallel; i++)
                r[i] = new Random(i);

            int cnt = 0;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (true)
            {
                for (int i = 0; i < issueParallel; i++)
                    tasks[i] = ReadIssuer(r[i]);
                Task.WaitAll(tasks);
            }

            async Task ReadIssuer(Random r)
            {
                await Task.Yield();

                int batchSize = 1000;
                var readTasks = new ValueTask<(Status, string)>[batchSize];
                for (int i = 0; i < batchSize; i++)
                {
                    readTasks[i] = mixedStorageDictionary.ReadAsync(r.Next(numRecords));
                }
                for (int i = 0; i < batchSize; i++)
                    await readTasks[i].ConfigureAwait(false);

                if (Interlocked.Increment(ref cnt) % 1000 == 0)
                {
                    sw.Stop();
                    Console.WriteLine($"Time for {1000 * batchSize} ops = {sw.ElapsedMilliseconds}ms");
                    sw.Restart();
                }
            }
        }
    }
}
