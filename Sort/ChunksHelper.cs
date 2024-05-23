namespace Sort
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public static class ChunksHelper
    {
        public static List<long> CreateIntermediateChunkFile(string sourceFilePath, string resultFileName, int bufferSize, ArrayPool<char> arrayPool)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            Console.WriteLine("Creation of sorted chunks file in progress");

            using var file = File.Open(
                sourceFilePath,
                new FileStreamOptions()
                {
                    Access = FileAccess.Read,
                    Options = FileOptions.SequentialScan,
                    Mode = FileMode.Open,
                });
            using var streamReader = new StreamReader(file, null!, false, 1_048_576);

            bufferSize /= sizeof(char);
            bufferSize = file.Length > bufferSize ?
                bufferSize : (int)file.Length;

            var resultPositionList = new List<long>() { 0 };
            long start = 0;
            while (start < streamReader.BaseStream.Length - 1)
            {
                var (resultPosition, originalPosition) = Process(streamReader, start, bufferSize, arrayPool, resultFileName);
                resultPositionList.Add(resultPosition);
                start += originalPosition;
            }

            stopwatch.Stop();
            Console.WriteLine($"Sorted chunks file created in {stopwatch.Elapsed}");

            return resultPositionList;
        }

        private static (long resultPosition, long originalPosition) Process(StreamReader reader, long position, int bufferSize, ArrayPool<char> arrayPool, string fileName)
        {
            bufferSize = reader.BaseStream.Length - position > bufferSize ?
                bufferSize : (int)(reader.BaseStream.Length - position);
            var buffer = arrayPool.Rent(bufferSize);

            FillBuffer(reader, position, buffer);
            List<Line> result = ProcessBufferIntoLines(position, buffer);
            Line[] resultBuffer = SortResults(result);
            long lastPosition = WriteResults(position, buffer, resultBuffer, fileName);

            int originalBufferPosition = new Span<char>(buffer).LastIndexOf('\n');
            originalBufferPosition = originalBufferPosition == -1 ?
                buffer.Length - 1 : originalBufferPosition + 1;
            arrayPool.Return(buffer, true);
            return (lastPosition, originalBufferPosition);
        }

        private static long WriteResults(long position, char[] buffer, Line[] resultBuffer, string fileName)
        {
            long lastPosition = 0;
            using (var streamWriter = new StreamWriter(fileName, true))
            {
                var memoryBuffer = new Memory<char>(buffer);
                foreach (var item in resultBuffer!)
                {
                    var slice = memoryBuffer.Slice((int)(item.wordPosition - position), item.wordLength);
                    streamWriter.WriteLine(slice);
                }
                streamWriter.Flush();
                lastPosition = streamWriter.BaseStream.Position;
            }

            return lastPosition;
        }

        private static Line[] SortResults(List<Line> result)
        {
            return result
                .AsParallel()
                .OrderBy(x => x.number)
                .ToArray();
        }

        private static List<Line> ProcessBufferIntoLines(long position, char[] buffer)
        {
            var result = new List<Line>();

            var bufferSpan = new Span<char>(buffer);
            int lastIndex = bufferSpan.IndexOf('\0') == -1 ?
                bufferSpan.LastIndexOf('\r') : bufferSpan.IndexOf('\0');
            bufferSpan = bufferSpan.Slice(0, lastIndex);
            long wordStartPosition = char.IsNumber(bufferSpan[0]) ?
                position : position + bufferSpan.IndexOf('\n') + 1; // there will be stored position of a word in buffer, not the actual one
            foreach (var ch in bufferSpan.EnumerateLines())
            {
                if (ch.Length == 0)
                {
                    continue;
                }
                var separator = ch.LastIndexOf('.');
                if (separator == -1)
                {
                    continue;
                }
                var number = long.Parse(ch.Slice(0, separator));
                result.Add(new Line(number, wordStartPosition, ch.Length));
                wordStartPosition += ch.Length + 2; // +2 to skip \r\n
            }

            return result;
        }

        private static void FillBuffer(StreamReader reader, long position, char[] buffer)
        {
            reader.BaseStream.Position = position;
            reader.DiscardBufferedData();
            reader.Read(buffer, 0, buffer.Length);
        }
    }

    public record struct Line(long number, long wordPosition, int wordLength)
    {
        public readonly long number = number;
        public readonly long wordPosition = wordPosition;
        public readonly int wordLength = wordLength;

        public override string ToString()
        {
            return $"{number} {wordPosition} {wordLength}";
        }
    }
}
