﻿using System;
using System.IO;
using System.Reflection;

using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using log4net.Repository;

namespace Deveel.Data.Diagnostics {
	[LoggerName("log4j")]
	public sealed class Log4NetLogger : ILogger {
		private log4net.ILog log;

		public void Dispose() {
			log = null;
		}

		private static Level GetLogLevel(ConfigSource config) {
			ILoggerRepository repository = LoggerManager.GetRepository(Assembly.GetCallingAssembly());
			Level level = Level.Off;
			string val = config.GetString("log_level");
			if (val != null) {
				level = repository.LevelMap[val];
				if (level == null) {
					int index = val.IndexOf(':');
					if (index == -1)
						throw new Exception("The 'log_level' configuration must specify a name and a numeric level.");
					string levelName = val.Substring(0, index);
					string sLevelValue = val.Substring(index + 1);
					int levelValue;
					if (Int32.TryParse(sLevelValue, out levelValue))
						repository.LevelMap.Add(levelName, levelValue);
				}
			}

			return level;
		}

		private static IAppender GetFileAppender(string loggerName, ConfigSource config, Level level) {
			// Logging directory,
			string value = config.GetString("log_directory");
			if (value == null)
				return null;

			// Set a log directory,
			string f = value.Trim();
			if (!Directory.Exists(f))
				Directory.CreateDirectory(f);

			value = value.Replace("\\", "/");
			if (!value.EndsWith("/"))
				value = value + "/";

			string fileName = config.GetString("log_file", null);
			if (fileName == null)
				fileName = "node.log";

			value = Path.GetFullPath(Path.Combine(value, fileName));

			if (!File.Exists(value))
				File.Create(value);

			// Output to the log file,
			RollingFileAppender appender = new RollingFileAppender();
			appender.RollingStyle = RollingFileAppender.RollingMode.Size;
			appender.Name = loggerName;
			appender.File = value;
			appender.AppendToFile = true;

			string logPattern = config.GetString("log_pattern", null);
			if (logPattern == null)
				logPattern = "%date [%thread] %-5level %logger - %message%newline";

			PatternLayout layout = new PatternLayout(logPattern);
			layout.ActivateOptions();
			appender.Layout = layout;
			appender.Threshold = level;
			appender.StaticLogFileName = true;
			appender.LockingModel = new FileAppender.MinimalLock();
			appender.ActivateOptions();

			return appender;
		}

		private static IAppender GetConsoleAppender(string  loggerName, ConfigSource config, Level level) {
			throw new NotImplementedException();
		}

		private Level ConvertToLevel(LogLevel l) {
			return log.Logger.Repository.LevelMap[l.Name];
		}

		public void Init(ConfigSource config) {
			string loggerName = config.GetString(Logger.LoggerNameKey);
			if (loggerName == null)
				throw new InvalidOperationException();

			Level level = GetLogLevel(config);
			IAppender appender = null;

			string logOutput = config.GetString("log_output");
			if (logOutput == null || 
				logOutput.Equals("console", StringComparison.InvariantCultureIgnoreCase)) {
				appender = GetConsoleAppender(loggerName, config, level);
			} else if (logOutput.Equals("file")) {
				appender = GetFileAppender(loggerName, config, level);
			}

			if (appender != null) {
				BasicConfigurator.Configure(appender);

				log = log4net.LogManager.GetLogger(loggerName);
			}
		}

		public bool IsInterestedIn(LogLevel level) {
			Level l = ConvertToLevel(level);
			return log.Logger.IsEnabledFor(l);
		}

		public void Write(LogLevel level, object ob, string message) {
			Level l = ConvertToLevel(level);
			log.Logger.Log(ob.GetType(), l, message, null);
		}

		public void Write(LogLevel level, Type type, string message) {
			Level l = ConvertToLevel(level);
			log.Logger.Log(type, l, message, null);
		}

		public void Write(LogLevel level, string typeString, string message) {
			Level l = ConvertToLevel(level);
			Type type = Type.GetType(typeString, true, true);
			log.Logger.Log(type, l, message, null);
		}

		public void WriteException(Exception e) {
			log.Logger.Log(null, Level.Error, e.Message, e);
		}

		public void WriteException(LogLevel level, Exception e) {
			Level l = ConvertToLevel(level);
			log.Logger.Log(null, l, e.Message, e);
		}
	}
}