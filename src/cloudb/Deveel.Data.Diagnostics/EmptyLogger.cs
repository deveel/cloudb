using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	internal class EmptyLogger : ILogger {
		public void Dispose() {
		}

		public void Init(ConfigSource config) {
		}

		public bool IsInterestedIn(LogLevel level) {
			return false;
		}

		public void Log(LogEntry entry) {
		}
	}
}