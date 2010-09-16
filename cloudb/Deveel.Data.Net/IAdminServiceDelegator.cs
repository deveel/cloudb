using System;

namespace Deveel.Data.Net {
	public interface IAdminServiceDelegator : IDisposable {		
		void Init(AdminService adminService);
		
		IService GetService(ServiceType serviceType);
		
		IService CreateService(IServiceAddress address, ServiceType serviceType, IServiceConnector connector);
		
		void DisposeService(ServiceType serviceType);
	}
}