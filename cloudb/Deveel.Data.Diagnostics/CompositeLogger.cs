﻿using System;
using System.Collections.Generic;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	[LoggerTypeName("composite")]
	public sealed class CompositeLogger : ILogger {
		private List<Logger> loggers = new List<Logger>();
		
		private void EnsureListCapacity(int offset) {
			if (offset >= loggers.Count) {
				for (int i = loggers.Count; i < offset; i++)
					loggers.Add(null);
			}
		}
		
		public void Init(ConfigSource config) {
			if (config.ChildCount == 0)
				return;
			
			foreach(ConfigSource child in config.Children) {
				string childName = child.Name;
				
				int offset = -1;
				if (!Int32.TryParse(childName, out offset))
					continue;
				
				if (offset < 1)
					continue;
				
				offset = offset -1;
				
				string refLogger = child.GetString("ref", null);
				if (!String.IsNullOrEmpty(refLogger)) {
					loggers.Insert(offset, Logger.GetLogger(refLogger));
				} else {
					string loggerTypeString = child.GetString("type", null);
					if (String.IsNullOrEmpty(loggerTypeString))
						loggerTypeString = "default";
					
					Type loggerType = Logger.GetLoggerType(loggerTypeString);
					if (loggerType == null || !typeof(ILogger).IsAssignableFrom(loggerType))
						continue;
						
					try {
						ILogger logger = (ILogger) Activator.CreateInstance(loggerType, true);
						logger.Init(child);
						
						EnsureListCapacity(offset);
						loggers.Insert(offset, new Logger(child.Name, logger, child));
					} catch {
						continue;
					}
				}
			}
		}
		
		public bool IsInterestedIn(LogLevel level) {
			for (int i = 0; i < loggers.Count; i++) {
				ILogger logger = loggers[i];
				if (logger != null && logger.IsInterestedIn(level))
					return true;
			}
			
			return false;
		}
		
		public void Log(LogEntry entry) {
			for (int i = 0; i < loggers.Count; i++) {
				ILogger logger = loggers[i];
				if (loggers != null)
					logger.Log(entry);
			}
		}
		
		public void Dispose() {
			for (int i = 0; i < loggers.Count; i++) {
				ILogger logger = loggers[i];
				if (logger != null)
					logger.Dispose();
			}
			
			loggers.Clear();
			loggers = null;
		}
	}
}