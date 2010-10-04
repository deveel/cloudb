using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;

namespace Deveel.Data.Diagnostics {
	public static class LogManager {
		private static readonly Dictionary<string, ILogger> loggers = new Dictionary<string, ILogger>(128);
		private static readonly Dictionary<string, Type> loggerTypeMap = new Dictionary<string, Type>(128);

		private static readonly object logSyncLock = new object();

		public const string LoggerNameKey = "log_name";
		public const string NetworkLoggerName = "network_log";
		public const string StorageLoggerName = "store_log";
		
		public static Logger NetworkLogger {
			get { return GetLogger(NetworkLoggerName); }
		}
		public static Logger StorageLogger {
			get { return GetLogger(StorageLoggerName); }
		}

		private static void InspectLoggers() {
			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			for (int i = 0; i < assemblies.Length; i++) {
				Assembly assembly = assemblies[i];
				Type[] types = assembly.GetTypes();
				for (int j = 0; j < types.Length; j++) {
					Type type = types[j];
					if (typeof(ILogger).IsAssignableFrom(type) &&
						type != typeof(ILogger) && 
						type != typeof(Logger) &&
						!type.IsAbstract) {
						LoggerNameAttribute nameAttribute =
							(LoggerNameAttribute) Attribute.GetCustomAttribute(type, typeof(LoggerNameAttribute));
						if (nameAttribute != null && 
							!loggerTypeMap.ContainsKey(nameAttribute.Name))
							loggerTypeMap[nameAttribute.Name] = type;
					}
				}
			}
		}

		private static Type GetLoggerType(string typeName) {
			Type type;
			if (!loggerTypeMap.TryGetValue(typeName, out type)) {
				type = Type.GetType(typeName, false, true);
				if (type != null)
					loggerTypeMap[typeName] = type;
			}
			return type;
		}

		public static void Init(ConfigSource config) {
			lock(logSyncLock) {
				InspectLoggers();

				string ln = config.GetString("logger", null);
				if (ln == null)
					ln = config.GetString("loggers", null);
				
				List<string> loggerNames = new List<string>();
				loggerNames.Add(NetworkLoggerName);
				loggerNames.Add(StorageLoggerName);
				
				if (ln != null) {
					string[] sp = ln.Split(',');
					for (int i = 0; i < sp.Length; i++) {
						loggerNames.Add(sp[i].Trim());
					}
				}
				for (int i = 0; i < loggerNames.Count; i++) {
					string loggerName = loggerNames[i];
					ConfigSource loggerConfig = new ConfigSource();
					foreach(string key in config.Keys) {
						if (key.StartsWith(loggerName + "_")) {
							string value = config.GetString(key);
							string loggerKey = key.Substring(loggerName.Length + 1, key.Length - (loggerName.Length + 1));
							loggerConfig.SetValue(loggerKey, value);
						}
					}

					loggerConfig.SetValue(LoggerNameKey, loggerName);

					Type loggerType;
					if (!loggerTypeMap.TryGetValue(loggerName, out loggerType)) {
						string loggerTypeName = loggerConfig.GetString("type", null);
						if (loggerTypeName == null)
							loggerTypeName = typeof(DefaultLogger).AssemblyQualifiedName;

						loggerType = GetLoggerType(loggerTypeName);
						if (loggerType == null || !typeof(ILogger).IsAssignableFrom(loggerType))
							continue;
					}
					try {
						ILogger logger = (ILogger) Activator.CreateInstance(loggerType, true);
						logger.Init(loggerConfig);
						loggers[loggerName] = logger;
					} catch {
						continue;
					}
				}
			}
		}

		public static Logger GetLogger(string logName) {
			ILogger logger;
			if (!loggers.TryGetValue(logName, out logger))
				logger = new EmptyLogger();
			return new Logger(logName, logger);
		}
	}
}