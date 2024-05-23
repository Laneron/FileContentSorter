namespace CreateBigFile
{
    using System;
    using System.Diagnostics;
    using System.Text;

    internal class Program
    {
        private readonly static string[] strings = {
            "apple", "banana", "cherry", "dog", "elephant",
            "fox", "grape", "hat", "ice cream", "jelly",
            "kiwi", "lemon", "monkey", "nut", "orange",
            "pear", "quilt", "rabbit", "snake", "tiger"
        };

        static async Task Main(string[] args)
        {
            if (args.Length < 3)
            {
                throw new ArgumentException("The buffer, file sizes and thread quantity must be defined.");
            }

            if (!int.TryParse(args[0], out int bufferSize))
            {
                throw new ArgumentException("Buffer size must be defined as integer.");
            }

            if (!long.TryParse(args[1], out var fileSize))
            {
                throw new ArgumentException("File size must be defined as integer.");
            }

            if (!int.TryParse(args[2], out var threadNumber))
            {
                throw new ArgumentException("Thread quantity must be defined as integer.");
            }

            CreateFile(bufferSize, fileSize, threadNumber);

        }

        private static void CreateFile(
            int bufferSize,
            long fileSize,
            int threadNumber)
        {
            var random = new Random();
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            long contentLength = 0;
            //using (var file = File.Create(@"./bigfile.txt", bufferSize))
            object sync = new object();
            using (var writer = new StreamWriter(@"./bigfile.txt", false, Encoding.UTF8, bufferSize))
            {
                var tasks = new Task[threadNumber];
                for (int i = 0; i < tasks.Length; i++)
                {
                    //tasks[i] = Write(writer, fileSize, random);
                    tasks[i] = Task.Run(() =>
                    {
                        var stringBuilder = new StringBuilder();
                        while (writer.BaseStream.Length < fileSize)
                        {
                            stringBuilder.Append(random.Next());
                            stringBuilder.Append(". ");
                            stringBuilder.Append(
                                strings[random.Next(0, strings.Length - 1)]
                                );
                            stringBuilder.AppendLine();

                            lock (sync)
                            {
                                writer.Write(stringBuilder.ToString());
                            }

                            stringBuilder.Clear();

                        }
                    });
                }

                do
                {
                    double approximateCurrentLengthGb = writer.BaseStream.Length / (1024.0 * 1024 * 1024);
                    Console.Write($"The file size of {approximateCurrentLengthGb:F2} GB");
                    Console.SetCursorPosition(0, Console.CursorTop);
                    Thread.Sleep(17);
                } while (tasks.All(x => x.Status == TaskStatus.Running || x.Status == TaskStatus.WaitingToRun || x.Status == TaskStatus.WaitingForActivation));
                //Task.WaitAll(tasks);
                contentLength = writer.BaseStream.Length;
            }
            stopwatch.Stop();

            Console.WriteLine($"\nThe file size of {contentLength} was created in {stopwatch.Elapsed}");
        }
    }
}
