using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace RExecutionActivityStartup
{
    class ConsoleLogger : IActivityLogger
    {
        public void Write(string format, params object[] args)
        {
            Console.WriteLine(format);
        }
    }
}
