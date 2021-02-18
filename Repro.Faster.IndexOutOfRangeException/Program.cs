using FASTER.core;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Repro.Faster.IndexOutOfRangeException
{
    public class Program
    {
        public static async Task Main()
        {
            var mixedStorageDictionary = new MixedStorageDictionary<int, string>();
            int numRecords = 1000;
            string dummyRecordValue = "dummyString";

            // Concurrent upserts
            var upsertTasks = new Task[numRecords];
            for (int i = 0; i < numRecords; i++)
            {
                int key = i;
                upsertTasks[i] = Task.Run(() => mixedStorageDictionary.Upsert(key, dummyRecordValue));
            }
            await Task.WhenAll(upsertTasks).ConfigureAwait(false);

            // Concurrent reads
            var readTasks = new Task<(Status, string)>[numRecords];
            for (int i = 0; i < numRecords; i++)
            {
                readTasks[i] = mixedStorageDictionary.ReadAsync(i).AsTask();
            }
            await Task.WhenAll(readTasks).ConfigureAwait(false);

            // Assert
            for (int i = 0; i < numRecords; i++)
            {
                (Status status, string result) = readTasks[i].Result;
                Assert.Equal(Status.OK, status);
                Assert.Equal(dummyRecordValue, result);
            }

            Console.WriteLine("Success!");
        }
    }
}
