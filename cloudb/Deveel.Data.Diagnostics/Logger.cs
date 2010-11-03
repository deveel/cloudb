using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	public sealed class Logger : ILogger, ICloneable {
		internal Logger(string name, ILogger logger, ConfigSource config) {
			this.name = name;
			this.logger = logger;
			this.config = config;
			
			logDelegate = new LogDelegate(Log);
		}

		private readonly string name;
		private ILogger logger;
		private readonly ConfigSource config;
		private bool async;
		
		private readonly LogDelegate logDelegate;
		
		private static readonly Dictionary<string, Logger> loggers = new Dictionary<string, Logger>(128);
		private static readonly Dictionary<string, Type> loggerTypeMap = new Dictionary<string, Type>(128);

		private static readonly object logSyncLock = new object();
		private static bool initid;

		public const string NetworkLoggerName = "network";
		public const string StoreLoggerName = "store";
		public const string ClientLoggerName = "client";
		
		public static Logger Network {
			get { return GetLogger(NetworkLoggerName); }
		}
		public static Logger Store {
			get { return GetLogger(StoreLoggerName); }
		}

		public static Logger Client {
			get { return GetLogger(ClientLoggerName); }
		}

		public string Name {
			get { return name; }
		}
		
		public bool Async {
			get { return async; }
			set { async = value; }
		}
		
		public ConfigSource Config {
			get { return config; }
		}

		public ILogger BaseLogger {
			get { return logger; }
		}
		
		public bool IsEmpty {
			get { return logger is EmptyLogger; }
		}
		
		public static bool IsInitialized {
			get { return initid; }
		}

		public void Dispose() {
			if (logger != null) {
				logger.Dispose();
				logger = null;
			}
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
		
		public IAsyncResult BeginLog(LogEntry entry, AsyncCallback callback, object state) {
			return logDelegate.BeginInvoke(entry, callback, state);
		}
		
		public void EndLog(IAsyncResult result) {
			logDelegate.EndInvoke(result);
		}
		
		private void DoLog(LogEntry entry) {
			string threadName = entry.Thread;
			if (String.IsNullOrEmpty(threadName))
				threadName = Thread.CurrentThread.Name;
			BaseLogger.Log(new LogEntry(threadName, entry.Source, entry.Level, entry.Message, entry.Error, entry.Time));
		}

		private static void InspectLoggers() {
			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			for (int i = 0; i < assemblies.Length; i++) {
				Assembly assembly = assemblies[i];
				Type[] types = assembly.GetTypes();
				for (int j = 0; j < types.Length; j++) {
					Type type = types[j];
					if (typeof(ILogger).IsAssignableFrom(type) &&
						type != typeof(ILogger) && type != typeof(Logger) &&
						!type.IsAbstract) {
						LoggerTypeNameAttribute nameAttribute =
							(LoggerTypeNameAttribute) Attribute.GetCustomAttribute(type, typeof(LoggerTypeNameAttribute));
						if (nameAttribute != null && 
							!loggerTypeMap.ContainsKey(nameAttribute.Name))
							loggerTypeMap[nameAttribute.Name] = type;
					}
				}
			}
		}

		public void Log(LogEntry entry) {
			if (async) {
				BeginLog(entry, null, null);
			} else {
				DoLog(entry);
			}
		}
		
		internal static Type GetLoggerType(string typeName) {
			Type type;
			if (!loggerTypeMap.TryGetValue(typeName, out type)) {
				type = Type.GetType(typeName, false, true);
				if (type != null)
					loggerTypeMap[typeName] = type;
			}
			return type;
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
		
		public object Clone() {
			ILogger log = logger;
			if (logger is ICloneable)
				log = (ILogger)((ICloneable)logger).Clone();
			return new Logger(name, log, (ConfigSource) config.Clone());
		}
		
		public static Logger GetLogger(string logName) {
			Logger logger;
			if (!loggers.TryGetValue(logName, out logger))
				logger = new Logger(logName, new EmptyLogger(), null);
			return logger;
		}
		
		public static void Init(ConfigSource config) {
			lock(logSyncLock) {
				InspectLoggers();
				
				ConfigSource loggerConfig = config;
				// if it is a root configuration, get the 'logger' child
				if (loggerConfig.Parent == null)
					loggerConfig = config.GetChild("logger");
				
				if (loggerConfig == null || loggerConfig.Name != "logger")
					throw new ArgumentException("The configuration is invalid.");
				
				if (loggerConfig.ChildCount == 0)
					return;
				
				List<ILogger> composites = new List<ILogger>();
				
				foreach(ConfigSource child in loggerConfig.Children) {
					string loggerTypeString = child.GetString("type");
					if (String.IsNullOrEmpty(loggerTypeString))
						loggerTypeString = "default";
					
					Type loggerType = GetLoggerType(loggerTypeString);
					if (loggerType == null || !typeof(ILogger).IsAssignableFrom(loggerType))
						continue;
						
					try {
						ILogger logger = (ILogger) Activator.CreateInstance(loggerType, true);
						if (logger is CompositeLogger) {
							// composite loggers are processed with a later bound call
							composites.Add(logger);
						} else {
							logger.Init(child);
						}
						
						loggers[child.Name] = new Logger(child.Name, logger, config);
					} catch {
						continue;
					}
				}
				
				// let's late process the composite loggers found
				if (composites.Count > 0) {
					for (int i = 0; i < composites.Count; i++) {
						composites[i].Init(config);
					}
				}
				
				initid = true;
			}
		}
		
		private delegate void LogDelegate(LogEntry entry);
	}
}