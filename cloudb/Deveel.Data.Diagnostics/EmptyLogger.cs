using System;

namespace Deveel.Data.Diagnostics {
	internal class EmptyLogger : ILogger {
		public void Dispose() {
		}

		public void Init(ConfigSource config) {
		}

		public bool IsInterestedIn(LogLevel level) {
			return false;
		}

		public void Write(LogLevel level, object ob, string message) {
		}

		public void Write(LogLevel level, Type type, string message) {
		}

		public void Write(LogLevel level, string typeString, string message) {
		}

		public void WriteException(Exception e) {
		}

		public void WriteException(LogLevel level, Exception e) {
		}
	}
}