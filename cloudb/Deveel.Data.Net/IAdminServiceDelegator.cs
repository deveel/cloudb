using System;

namespace Deveel.Data.Net {
	public interface IAdminServiceDelegator : IDisposable {		
		void Init(AdminService adminService);
		
		IService GetService(ServiceType serviceType);
		
		void StartService(IServiceAddress address, ServiceType serviceType, IServiceConnector connector);
		
		void StopService(ServiceType serviceType);
	}
}