using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	public static class LogManager {
		private static readonly Dictionary<string, Logger> loggers = new Dictionary<string, Logger>(128);
		private static readonly Dictionary<string, Type> loggerTypeMap = new Dictionary<string, Type>(128);

		private static readonly object logSyncLock = new object();
		private static bool initid;

		public const string NetworkLoggerName = "network";
		public const string StorageLoggerName = "store";
		
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
						LoggerTypeNameAttribute nameAttribute =
							(LoggerTypeNameAttribute) Attribute.GetCustomAttribute(type, typeof(LoggerTypeNameAttribute));
						if (nameAttribute != null && 
							!loggerTypeMap.ContainsKey(nameAttribute.Name))
							loggerTypeMap[nameAttribute.Name] = type;
					}
				}
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
		
		public static void Init(ConfigSource config) {
			lock(logSyncLock) {
				if (initid)
					return;
				
				InspectLoggers();
				
				ConfigSource loggerConfig = config;
				// if it is a root configuration, get the 'logger' child
				if (loggerConfig.Parent == null)
					loggerConfig = config.GetChild("logger");
				
				if (loggerConfig == null || loggerConfig.Name != "logger")
					throw new ArgumentException("The configuration is invalid.");
				
				if (loggerConfig.ChildCount == 0)
					return;
				
				foreach(ConfigSource child in loggerConfig.Children) {
					string loggerTypeString = child.GetString("type");
					if (String.IsNullOrEmpty(loggerTypeString))
						loggerTypeString = "default";
					
					Type loggerType = GetLoggerType(loggerTypeString);
					if (loggerType == null || !typeof(ILogger).IsAssignableFrom(loggerType))
						continue;
						
					try {
						ILogger logger = (ILogger) Activator.CreateInstance(loggerType, true);
						logger.Init(child);
						loggers[child.Name] = new Logger(child.Name, logger, config);
					} catch {
						continue;
					}
				}
				
				initid = true;
			}
		}

		public static Logger GetLogger(string logName) {
			Logger logger;
			if (!loggers.TryGetValue(logName, out logger))
				logger = new Logger(logName, new EmptyLogger(), null);
			return logger;
		}
	}
}