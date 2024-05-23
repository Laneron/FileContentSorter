namespace Sort
{
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.CompilerServices;

    public class Queue
    {
        private FileValueLine[] _array;
        private int _head;
        private int _tail;
        private int _size;
        private Task<bool> _refillingTask;

        public Queue(uint capacity, Task<bool> refillingTask)
        {
            _array = new FileValueLine[capacity];
            _refillingTask = refillingTask;
            _refillingTask.Start();
        }

        public event Func<CancellationToken, Task<bool>> QueueNeedsRefilling;

        public bool TryEnqueue(FileValueLine item)
        {
            int localSize = Volatile.Read(ref _size);
            int localTail = Volatile.Read(ref _tail);
            if (localTail >= _array.Length)
            {
                _tail = 0;
                return false;
            }

            if (_head == _tail && localSize != 0)
            {
                return false;
            }

            _array[localTail] = item;

            Interlocked.Add(ref _tail, 1);
            Interlocked.Add(ref _size, 1);

            return true;
        }

        public bool TryDequeue(out FileValueLine value)
        {
            int localSize = Volatile.Read(ref _size);
            if (localSize <= _array.Length * 0.5)
            {
                if (_refillingTask == null || (_refillingTask.IsCompleted && _refillingTask.Result == true))
                {
                    _refillingTask = QueueNeedsRefilling(CancellationToken.None);
                    while (Volatile.Read(ref _size) == 0 || !_refillingTask.IsCompleted)
                        _refillingTask.Wait(10);
                }
                else
                {
                    localSize = Volatile.Read(ref _size);
                    if (localSize == 0)
                    {
                        if (_refillingTask.IsCompleted) // That's all for this queue
                        {
                            _refillingTask = null!;
                            value = default!;
                            return false;
                        }
                        else
                        {
                            while (_refillingTask.Status == TaskStatus.WaitingToRun)
                                _refillingTask.Wait(10);
                        }
                    }

                }
            }

            FileValueLine removed = _array[_head];
            if (RuntimeHelpers.IsReferenceOrContainsReferences<FileValueLine>())
            {
                _array[_head] = default!;
            }
            value = removed;

            Interlocked.Add(ref _head, 1);
            if (_head == _array.Length)
            {
                _head = 0;
            }
            Interlocked.Add(ref _size, -1);
            return true;
        }

        public bool TryPeek([MaybeNullWhen(false)] out FileValueLine result)
        {
            if (_size == 0)
            {
                if (_refillingTask != null
                    && !_refillingTask.IsCompleted)
                {
                    while (Volatile.Read(ref _size) == 0 || !_refillingTask.IsCompleted)
                        _refillingTask.Wait(10);

                    result = _array[_head];
                    return true;
                }

                result = default!;
                return false;
            }

            result = _array[_head];

            return true;
        }
    }
}
