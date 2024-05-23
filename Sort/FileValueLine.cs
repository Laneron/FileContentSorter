namespace Sort
{
    public readonly struct FileValueLine(long number, string str)
    {
        public readonly long number = number;
        public readonly string str = str;

        public override string ToString()
        {
            return $"{number}. {str}";
        }
    }
}
