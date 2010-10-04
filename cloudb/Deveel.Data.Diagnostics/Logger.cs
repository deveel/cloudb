using System;
using System.Diagnostics;
using System.Threading;

namespace Deveel.Data.Diagnostics {
	public sealed class Logger : ILogger {
		internal Logger(string name, ILogger logger) {
			this.name = name;
			this.logger = logger;
		}

		private readonly string name;
		private readonly ILogger logger;

		public string Name {
			get { return name; }
		}

		public ILogger BaseLogger {
			get { return logger; }
		}

		public void Dispose() {
			BaseLogger.Dispose();
		}

		void ILogger.Init(ConfigSource config) {
			throw new InvalidOperationException();
		}
		
		private Type GetLoggingType() {
			return new StackFrame(2, false).GetMethod().DeclaringType;
		}

		public bool IsInterestedIn(LogLevel level) {
			return BaseLogger.IsInterestedIn(level);
		}

		private void Log(LogEntry entry) {
			string threadName = entry.Thread;
			if (String.IsNullOrEmpty(threadName))
				threadName = Thread.CurrentThread.Name;
			BaseLogger.Log(new LogEntry(threadName, entry.Source, entry.Level, entry.Message, entry.Error, entry.Time));
		}

		void ILogger.Log(LogEntry entry) {
			throw new InvalidOperationException();
		}

		public void Log(LogLevel level, object ob, string message) {
			Log(new LogEntry(null, ob.GetType().AssemblyQualifiedName, level, message, null, DateTime.Now));
		}

		public void Log(LogLevel level, Type type, string message) {
			Log(new LogEntry(null, type.AssemblyQualifiedName, level, message, null, DateTime.Now));
		}

		public void Log(LogLevel level, string typeString, string message) {
			Log(new LogEntry(null, typeString, level, message, null, DateTime.Now));
		}

		public void Log(LogLevel level, string message, Exception error) {
			Log(new LogEntry(null, GetLoggingType().AssemblyQualifiedName, level, message, error, DateTime.Now));
		}

		public void Log(LogLevel level, object ob, string message, Exception error) {
			Log(new LogEntry(null, ob.GetType().AssemblyQualifiedName, level, message, error, DateTime.Now));
		}

		public void Log(LogLevel level, Type type, string message, Exception error) {
			Log(new LogEntry(null, type.AssemblyQualifiedName, level, message, error, DateTime.Now));
		}

		public void Log(LogLevel level, string typeString, string message, Exception error) {
			Log(new LogEntry(null, typeString, level, message, error, DateTime.Now));
		}

		public void Log(LogLevel level, string message) {
			Log(new LogEntry(null, GetLoggingType().AssemblyQualifiedName, level, message, null, DateTime.Now));
		}


		public void Log(LogLevel level, Exception e) {
			Log(new LogEntry(null, GetLoggingType().AssemblyQualifiedName, level, null, e, DateTime.Now));
		}

		public void Error(object ob, string message) {
			Log(LogLevel.Error, ob, message);
		}

		public void Error(Type type, string message) {
			Log(LogLevel.Error, type, message);
		}

		public void Error(string typeString, string message) {
			Log(LogLevel.Error, typeString, message);
		}
		
		public void Error(Type type, Exception error) {
			Log(LogLevel.Error, type, null, error);
		}

		public void Error(string  message) {
			Log(LogLevel.Error, message);
		}

		public void Error(object ob, string message, Exception error) {
			Log(LogLevel.Error, ob, message, error);
		}

		public void Error(Type type, string message, Exception error) {
			Log(LogLevel.Error, type, message, error);
		}

		public void Error(string typeString, string message, Exception error) {
			Log(LogLevel.Error, typeString, message, error);
		}

		public void Error(string message, Exception error) {
			Type loggingType = GetLoggingType();
			Log(LogLevel.Error, loggingType, message, error);
		}

		public void Error(Exception e) {
			Type loggingType = GetLoggingType();
			Error(loggingType, e);
		}

		public void Warning(object ob, string message) {
			Log(LogLevel.Warning, ob, message);
		}

		public void Warning(Type type, string message) {
			Log(LogLevel.Warning, type, message);
		}

		public void Warning(string typeString, string message) {
			Log(LogLevel.Warning, typeString, message);
		}

		public void Warning(Exception e) {
			Log(LogLevel.Warning, GetLoggingType(), null, e);
		}
		
		public void Warning(string message) {
			Log(LogLevel.Warning, GetLoggingType(), message);
		}

		public void Info(object ob, string message) {
			Log(LogLevel.Information, ob, message);
		}

		public void Info(Type type, string message) {
			Log(LogLevel.Information, type, message);
		}

		public void Info(string typeString, string  message) {
			Log(LogLevel.Information, typeString, message);
		}

		public void Info(Exception e) {
			Log(LogLevel.Information, GetLoggingType(), null, e);
		}
		
		public void Info(string message) {
			Log(LogLevel.Information, GetLoggingType(), message);
		}
	}
}