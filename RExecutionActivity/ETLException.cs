using System;

namespace ExecuteRScriptWithCustomActivity
{
    internal class ETLException : Exception
    {
        public ETLException(string message) : base(message)
        {
        }
    }
}