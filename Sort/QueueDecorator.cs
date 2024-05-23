namespace Sort
{
    using System;
    using System.Buffers;
    using System.IO;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;


    /// <summary>
    /// Class to fill up stored queue
    /// </summary>
    class QueueDecorator : IDisposable
    {
        private readonly Sort.Queue queue;
        private readonly StreamReader reader;
        private readonly FileStream file;
        private readonly long lastPosition;
        private readonly int defaultBufferSize;
        private readonly ArrayPool<char> arrayPool;

        private long currentFilePosition;
        private char[] buffer;

        public QueueDecorator(long startFilePosition, long lastPosition, int defaultBufferSize, ArrayPool<char> arrayPool, string filePath)
        {
            this.currentFilePosition = startFilePosition;

            var fileStreamOptions = new FileStreamOptions()
            {
                Access = FileAccess.Read,
                Mode = FileMode.Open,
                Options = FileOptions.SequentialScan
            };
            file = File.Open(filePath, fileStreamOptions);
            reader = new StreamReader(file, null!, true, 1_048_576);
            reader.BaseStream.Position = currentFilePosition;

            this.lastPosition = lastPosition;
            this.arrayPool = arrayPool;
            this.defaultBufferSize = defaultBufferSize;

            var bufferSize = (uint)(defaultBufferSize / Marshal.SizeOf<FileValueLine>());
            bufferSize = bufferSize > 0 ? bufferSize : 1024;

            queue = new Queue(bufferSize, new Task<bool>(() => this.Update()));
            queue.QueueNeedsRefilling += (cancellationToken) =>
            {
                return Task.Run(() =>
                {
                    return Update();
                });
            };
        }

        /// <summary>
        /// Load inner buffer
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool Update()
        {
            if (currentFilePosition > lastPosition)
            {
                return false; // Is allowed to continue
            }

            this.reader.BaseStream.Position = currentFilePosition;
            this.reader.DiscardBufferedData();

            int bufferSize;
            if (currentFilePosition + defaultBufferSize > lastPosition)
            {
                bufferSize = (int)(lastPosition - currentFilePosition);
            }
            else
            {
                bufferSize = defaultBufferSize;
            }

            this.buffer = arrayPool.Rent(bufferSize); // take new buffer from arrayPool to avoid possible allocations and get empty array
            this.reader.Read(buffer, 0, bufferSize);
            var memory = new Memory<char>(buffer, 0, bufferSize);
            memory.Trim();

            int filePositionInc = Process(memory);
            if (filePositionInc == 0
                && (currentFilePosition + defaultBufferSize) > lastPosition)
            {
                return false;
            }
            currentFilePosition += filePositionInc;
            arrayPool.Return(buffer, true); // return unused to avoid possible overlaps between concurrent readers

            return true; // Is allowed to continue
        }


        /// <summary>
        /// Process given buffer
        /// </summary>
        /// <param name="memory"></param>
        /// <returns>First index of next line</returns>
        private int Process(Memory<char> memory)
        {
            var span = memory.Span;

            int lastBufferIndex = span.LastIndexOf('\r');
            if (lastBufferIndex == -1)
            {
                lastBufferIndex = memory.Length;
            }
            span = span.Slice(0, lastBufferIndex);

            int offset = 0;
            foreach (var line in span.EnumerateLines())
            {
                var separator = line.LastIndexOf('.');
                if (line.Length == 0 || separator == -1) // in case of trailing line endings
                {
                    continue;
                }

                long number = long.Parse(line.Slice(0, separator));
                var str = new string(line.Slice(separator + 2)); // to skip whitespace after period charater

                if (!queue.TryEnqueue(new FileValueLine(number, str)))
                {
                    break;
                }

                offset += line.Length + 2; // +2 to compensate \r\n
            }

            return offset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Enqueue(FileValueLine value)
        {
            return queue.TryEnqueue(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out FileValueLine value)
        {
            return queue.TryDequeue(out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPeek(out FileValueLine value)
        {
            return queue.TryPeek(out value);
        }

        public void Dispose()
        {
            file.Dispose();
            reader.Dispose();
        }
    }
}
