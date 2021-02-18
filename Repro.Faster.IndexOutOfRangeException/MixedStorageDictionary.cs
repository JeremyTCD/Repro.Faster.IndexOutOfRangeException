using FASTER.core;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace Repro.Faster.IndexOutOfRangeException
{
    public class MixedStorageDictionary<TKey, TValue> : IMixedStorageDictionary<TKey, TValue>
    {
        // Serialization
        private static readonly MessagePackSerializerOptions _messagePackOptions = MessagePackSerializerOptions.
            Standard.
            WithCompression(MessagePackCompression.Lz4BlockArray);
        private static readonly SimpleFunctions<TKey, TValue> _simpleFunctions = new();

        // Sessions (1 per thread)
        [ThreadStatic]
        private static ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>? _session;

        // Disposal
        private static readonly ConcurrentBag<ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>>> _sessions = new();
        private readonly FasterKV<TKey, TValue> _store;
        private bool _disposed;

        public MixedStorageDictionary(string? logFileDirectory = null,
            string? logFileName = null,
            int pageSizeBits = 12, // 4 KB pages
            int inMemorySpaceBits = 13, // 2 pages
            long indexNumBuckets = 1L << 20) // 64 MB index
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
        }

        public void Upsert(TKey key, TValue obj)
        {
            (_session ?? CreateSession()).Upsert(key, obj);
        }

        public async ValueTask<(Status, TValue)> ReadAsync(TKey key)
        {
            return (await (_session ?? CreateSession()).ReadAsync(key).ConfigureAwait(false)).Complete();
        }

        private ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>> CreateSession()
        {
            _session = _store.For(_simpleFunctions).NewSession<SimpleFunctions<TKey, TValue>>();
            _sessions.Add(_session);

            return _session;
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

                    foreach (ClientSession<TKey, TValue, TValue, TValue, Empty, SimpleFunctions<TKey, TValue>> session in _sessions)
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
