using FASTER.core;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using static System.Threading.Interlocked;

namespace Repro.Faster.IndexOutOfRangeException
{
    public class MixedStorageDictionary<TKey, TValue> : IMixedStorageDictionary<TKey, TValue>
    {
        // Serialization
        private static readonly MessagePackSerializerOptions _messagePackOptions = MessagePackSerializerOptions.
            Standard.
            WithCompression(MessagePackCompression.Lz4BlockArray);
        private static readonly SimpleFunctions<TKey, TValue> _simpleFunctions = new();

        // Log compaction
        private readonly LogAccessor<TKey, TValue> _logAccessor;
        private readonly int _operationsBetweenLogCompactions;
        private int _operationsSinceLastLogCompaction = 0; // We're using interlocked so this doesn't need to be volatile

        // Thread local session
        [ThreadStatic]
        private static ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>? _session;

        // Shared pool
        private static readonly ConcurrentQueue<ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>> _sessions = new();

        // Disposal
        private static readonly ConcurrentBag<ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>> _allSessions = new();
        private readonly FasterKV<TKey, TValue> _store;
        private bool _disposed;

        public MixedStorageDictionary(string? logFileDirectory = null,
            string? logFileName = null,
            int pageSizeBits = 12, // 4 KB pages
            int inMemorySpaceBits = 13, // 2 pages
            long indexNumBuckets = 1L << 20, // 64 MB index
            int operationsBetweenLogCompactions = 1000) // Number of upserts and deletes between compaction attempts
        {
            var serializerSettings = new SerializerSettings<TKey, TValue>
            {
                valueSerializer = () => new Serializer()
            };

            logFileDirectory ??= Path.Combine(Path.GetTempPath(), "FasterLogs");
            logFileName ??= Guid.NewGuid().ToString();
            IDevice log = Devices.CreateLogDevice(Path.Combine(logFileDirectory, $"{logFileName}.log"));
            IDevice objlog = Devices.CreateLogDevice(Path.Combine(logFileDirectory, $"{logFileName}.obj.log"));

            var logSettings = new LogSettings
            {
                LogDevice = log,
                ObjectLogDevice = objlog,
                PageSizeBits = pageSizeBits,
                MemorySizeBits = inMemorySpaceBits
            };

            _store = new(size: indexNumBuckets,
                logSettings: logSettings,
                serializerSettings: serializerSettings);
            _logAccessor = _store.Log;
            _operationsBetweenLogCompactions = operationsBetweenLogCompactions;
        }

        public void Upsert(TKey key, TValue obj)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(MixedStorageDictionary<TKey, TValue>));
            }

            GetSession().Upsert(key, obj);
            CompactLogIfRequired();
        }

        public Status Delete(TKey key)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(MixedStorageDictionary<TKey, TValue>));
            }

            Status result = GetSession().Delete(key);
            CompactLogIfRequired();

            return result;
        }

        public async ValueTask<(Status, TValue)> ReadAsync(TKey key)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(MixedStorageDictionary<TKey, TValue>));
            }

            ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>? session = GetSession();
            ValueTask<FasterKV<TKey, TValue>.ReadAsyncResult<TValue, TValue, Empty>> readAsyncResult = session.ReadAsync(key);

            // Retain thread-local in sync path.
            // This speed ups in-memory reads at the cost of slightly slower disk reads.
            if (readAsyncResult.IsCompleted)
            {
                return readAsyncResult.Result.Complete();
            }

            // Going async - remove session from thread-local.
            // If we don't do this, the session is captured by the continuation, which may run on a different thread.
            // If that happens, we might erroneously use a session from multiple threads at the same time. 
            // - https://github.com/microsoft/FASTER/issues/403#issuecomment-781408293
            _session = null;
            (Status, TValue) result = (await readAsyncResult.ConfigureAwait(false)).Complete();

            // Return session to shared pool on async thread
            _sessions.Enqueue(session);
            return result;
        }

        private void CompactLogIfRequired()
        {
            // Faster log addresses:
            //
            // (oldest entries here) BeginAddress <= HeadAddress (where the in-memory region begins) <= SafeReadOnlyAddress (entries between here and tail updated in-place) < TailAddress (entries added here)
            //
            // If _operationsSinceLastLogCompaction < _operationsBetweenLogCompactions, it's not yet time to compact.
            // If _operationsSinceLastLogCompaction > _operationsBetweenLogCompactions, we're attempting to compact, do nothing.
            if (Increment(ref _operationsSinceLastLogCompaction) != _operationsBetweenLogCompactions)
            {
                return;
            }

            if (_logAccessor.BeginAddress == _logAccessor.SafeReadOnlyAddress) // All records fit within update-in-place region, nothing to compact
            {
                Exchange(ref _operationsSinceLastLogCompaction, 0);
                return;
            }

            // TOOD throws
            CompactLog();

            // Run asynchronously
            // Task.Run(CompactLog);
        }

        private void CompactLog()
        {
            long compactUntilAddress = (long)(_logAccessor.BeginAddress + 0.2 * (_logAccessor.SafeReadOnlyAddress - _logAccessor.BeginAddress));
            Console.WriteLine("compacting");
            // TODO throws
            _store.For(_simpleFunctions).NewSession<SimpleFunctions<TKey, TValue>>().Compact(compactUntilAddress, shiftBeginAddress: true);

            // GetSession().Compact(compactUntilAddress, shiftBeginAddress: true);
            Exchange(ref _operationsSinceLastLogCompaction, 0);
        }

        private ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>> GetSession()
        {
            if (_session != null)
            {
                return _session;
            }

            if (_sessions.TryDequeue(out ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>? result))
            {
                return _session = result;
            }

            ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>? session = _store.For(_simpleFunctions).NewSession<SimpleFunctions<TKey, TValue>>();
            _allSessions.Add(session);
            return _session = session;
        }

        private class Serializer : BinaryObjectSerializer<TValue>
        {
            public override void Deserialize(out TValue obj)
            {
                obj = MessagePackSerializer.Deserialize<TValue>(reader.BaseStream, _messagePackOptions);
            }

            public override void Serialize(ref TValue obj)
            {
                MessagePackSerializer.Serialize(writer.BaseStream, obj, _messagePackOptions);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _store.Dispose();

                    foreach (ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>> session in _allSessions)
                    {
                        session.Dispose();
                    }
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
