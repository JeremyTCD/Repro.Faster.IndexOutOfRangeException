using FASTER.core;
using System;
using System.Threading.Tasks;

namespace Repro.Faster.IndexOutOfRangeException
{
    public interface IMixedStorageDictionary<TKey, TValue> : IDisposable
    {
        void Dispose();
        ValueTask<(Status, TValue)> ReadAsync(TKey key);
        void Upsert(TKey key, TValue obj);
    }
}