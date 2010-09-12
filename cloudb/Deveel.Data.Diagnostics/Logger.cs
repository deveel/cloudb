using System;
using System.Collections.Generic;
using System.Reflection;

namespace Deveel.Data.Diagnostics {
	public static class Logger {
		private static readonly Dictionary<string, ILogger> loggers = new Dictionary<string, ILogger>(128);
		private static readonly Dictionary<string, Type> loggerTypeMap = new Dictionary<string, Type>(128);

		public const string LoggerNameKey = "log_name";

		private static void InspectLoggers() {
			Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
			for (int i = 0; i < assemblies.Length; i++) {
				Assembly assembly = assemblies[i];
				Type[] types = assembly.GetTypes();
				for (int j = 0; j < types.Length; j++) {
					Type type = types[j];
					if (typeof(ILogger).IsAssignableFrom(type) &&
						type != typeof(ILogger) &&
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
			if (loggerTypeMap.TryGetValue(typeName, out type))
				return type;
			return Type.GetType(typeName, false, true);
		}

		public static void Init(ConfigSource config) {
			InspectLoggers();

			string ln = config.GetString("logger", null);
			if (ln == null)
				ln = config.GetString("loggers", null);
			if (ln == null)
				return;

			string[] sp = ln.Split(',');
			for (int i = 0; i < sp.Length; i++) {
				string loggerName = sp[i].Trim();
				ConfigSource loggerConfig = new ConfigSource();
				foreach(string key in config.Keys) {
					if (key.StartsWith(loggerName + "_")) {
						string value = config.GetString(key);
						string loggerKey = key.Substring(loggerName.Length + 1, key.Length - (loggerName.Length + 1));
						loggerConfig.SetValue(loggerKey, value);
					}
				}

				loggerConfig.SetValue(LoggerNameKey, loggerName);

				string loggerTypeName = loggerConfig.GetString(loggerName + "_logger_type", null);
				if (loggerTypeName == null)
					loggerTypeName = typeof(DefaultLogger).AssemblyQualifiedName;

				Type loggerType = GetLoggerType(loggerTypeName);
				if (loggerType == null || !typeof(ILogger).IsAssignableFrom(loggerType))
					continue;

				try {
					ILogger logger = (ILogger)Activator.CreateInstance(loggerType, true);
					logger.Init(loggerConfig);
					loggers[loggerName] = logger;
				} catch {
					continue;
				}
			}
		}

		public static void Log(string loggerName, LogLevel level, Type sourceType, string message) {
			ILogger logger;
			if (loggers.TryGetValue(loggerName, out logger)) {
				if (logger.IsInterestedIn(level))
					logger.Write(level, sourceType, message);
			}
		}

		public static void Log(string loggerName, Exception exception) {
			ILogger logger;
			if (loggers.TryGetValue(loggerName, out logger)) {
				logger.WriteException(exception);
			}
		}

		public static void Log(string loggerName, LogLevel level, Exception exception) {
			ILogger logger;
			if (loggers.TryGetValue(loggerName, out logger)) {
				if (logger.IsInterestedIn(level))
					logger.WriteException(level, exception);
			}
		}
	}
}