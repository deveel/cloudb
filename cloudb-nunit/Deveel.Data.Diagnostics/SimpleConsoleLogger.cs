using System;
using System.IO;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	[LoggerTypeNameAttribute("simple-console")]
	public sealed class SimpleConsoleLogger : ILogger {
		public void Init(ConfigSource config) {
		}
		
		public bool IsInterestedIn(LogLevel level) {
			return true;
		}
		
		public void Log(LogEntry entry) {
			LogLevel level = entry.Level;
			Type source = Type.GetType(entry.Source);
			string message = String.Format("[{0}] [{1}] ({2}) {3} - {4}", entry.Thread, entry.Level, entry.Time, 
			                               (source.Namespace + "." + source.Name), entry.Message);
			TextWriter output = Console.Out;
			if (level >= LogLevel.Error)
				output = Console.Error;
			
			output.WriteLine(message);
			output.Flush();
		}
		
		public void Dispose() {
		}
	}
}