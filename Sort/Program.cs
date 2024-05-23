namespace Sort
{
    using System;
    using System.Buffers;
    using System.Diagnostics;
    using System.Text;

    internal partial class Program
    {
        public const string IntermediateChunksFilename = "sorted_chunks.txt";

        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                throw new Exception("Arguments are empty.");
            }

            string filePath = args[0];
            string resultFilePath = args[1];

            // buffer size in bytes
            if (!int.TryParse(args[2], out int bufferSize))
            {
                throw new Exception("Buffer size was incorrect format.");
            }

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            ArrayPool<char> arrayPool = ArrayPool<char>.Shared;
            List<long> resultPositionList = ChunksHelper.CreateIntermediateChunkFile(filePath, IntermediateChunksFilename, bufferSize, arrayPool);
            QueueDecorator[] queues = CreateProcessingQueues(arrayPool, resultPositionList, bufferSize);
            SortProcessingQueuesInFile(queues, resultFilePath);

            if (File.Exists(IntermediateChunksFilename))
                File.Delete(IntermediateChunksFilename);

            Console.WriteLine($"All done in {stopwatch.Elapsed}");
        }

        private static QueueDecorator[] CreateProcessingQueues(ArrayPool<char> arrayPool, List<long> resultPositionList, int bufferSize)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            Console.WriteLine("Starting configuration of processing queues");

            int processingBufferSize = GetProcessingBufferSize(bufferSize, resultPositionList);
            var queues = new QueueDecorator[resultPositionList.Count - 1];
            resultPositionList.Zip(resultPositionList.Skip(1), (x, y) => (x, y - 1)).Select((item, index) => (item, index)).ToList().ForEach(item =>
            {
                var queueFiller = new QueueDecorator(item.item.x, item.item.Item2, processingBufferSize, arrayPool, IntermediateChunksFilename);
                queues[item.index] = queueFiller;
                //queueFiller.Update();
            });

            stopwatch.Stop();
            Console.WriteLine($"Processing queues are configured and preloaded in {stopwatch.Elapsed}");
            return queues;
        }

        private static void SortProcessingQueuesInFile(QueueDecorator[] queues, string fileName)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            Console.WriteLine("Sorting in progress");
            using (var streamWriter = new StreamWriter(fileName, true, Encoding.UTF8, 1_485_760))
            {
                var cycleStopwatch = new Stopwatch();
                int resultIndex = 0;
                long counter = 0;
                while (true)
                {
                    resultIndex = LinearMinimal(queues).index;

                    if (!queues.Any(x => x != null))
                    {
                        break;
                    }

                    if (queues[resultIndex] == null)
                    {
                        queues[resultIndex].Dispose();
                        continue;
                    }

                    if (!queues[resultIndex].TryDequeue(out var lineStr))
                    {
                        queues[resultIndex] = null!;
                    }
                    else
                    {
                        streamWriter.WriteLine(lineStr.ToString()); // lazy option, rewrote to something not allocating unnecessary objects
                    }

                    counter++;
                    if (counter % 1_000_000 == 0)
                    {
                        stopwatch.Stop();
                        Console.SetCursorPosition(0, Console.CursorTop - 1);
                        Console.WriteLine($"{counter} lines ready in {cycleStopwatch.Elapsed}");
                        stopwatch.Start();
                    }
                }
            }

            Console.WriteLine($"Sorting completed in {stopwatch.Elapsed}");
        }

        private static (long value, int index) LinearMinimal(Memory<QueueDecorator> kResults)
        {
            long minimalValue = long.MaxValue;
            int resultIndex = 0;

            var span = kResults.Span;
            for (int ik = 0; ik < span.Length; ik++)
            {
                if (span[ik] == null)
                {
                    continue;
                }

                if (!span[ik].TryPeek(out var value))
                {
                    span[ik].Dispose();
                    span[ik] = null!;
                    continue;
                }

                if (value.number <= minimalValue)
                {
                    minimalValue = value.number;
                    resultIndex = ik;
                }
            }

            return (minimalValue, resultIndex);
        }

        private static int GetProcessingBufferSize(int bufferSize, List<long> resultPositionList)
        {
            int defaulBufferSize = (bufferSize / (resultPositionList.Count - 1));
            defaulBufferSize = defaulBufferSize > 1000 ? defaulBufferSize : 10000;
            return defaulBufferSize;
        }

    }
}
