// MzObj.h : Declaration of the CMzObj

#ifndef __MZOBJ_H_
#define __MZOBJ_H_

#include "resource.h"       // main symbols
#include "scheme.h"
#include "MzCOMCP.h"

typedef struct {
  BSTR **ppInput;
  BSTR *pOutput;
  HRESULT *pHr;
  HANDLE readSem;
  HANDLE writeSem;
  HANDLE resetSem;
  HANDLE resetDoneSem;
  BOOL *pErrorState;
  BOOL *pResetFlag;
} THREAD_GLOBALS;

extern const DWORD dwTimeOut;
extern const DWORD dwPause;
extern HINSTANCE globHinst;
extern DWORD WINAPI evalLoop(LPVOID);

/////////////////////////////////////////////////////////////////////////////
// CMzObj
class ATL_NO_VTABLE CMzObj : 
	public CComObjectRootEx<CComSingleThreadModel>,
	public CComCoClass<CMzObj, &CLSID_MzObj>,
	public IConnectionPointContainerImpl<CMzObj>,
	public IDispatchImpl<IMzObj, &IID_IMzObj, &LIBID_MZCOMLib>,
	public CProxy_IMzObjEvents< CMzObj >
{

  private:

    HRESULT hr;
    HANDLE inputMutex;
    HANDLE readSem;
    HANDLE writeSem;
    HANDLE resetSem;
    HANDLE resetDoneSem;
    HANDLE evalDoneSems[2];
    BSTR *globInput;
    BSTR globOutput;
    BSTR lastOutput;
    DWORD threadId;
    HANDLE threadHandle;
    BOOL errorState;
    
    void RaiseError(const OLECHAR *);
    BOOL testThread(void);
    void startMzThread(void);
    void killMzThread(void);

  public:

    CMzObj(void);
    ~CMzObj(void);

DECLARE_REGISTRY_RESOURCEID(IDR_MZOBJ)

DECLARE_PROTECT_FINAL_CONSTRUCT()

BEGIN_COM_MAP(CMzObj)
	COM_INTERFACE_ENTRY(IMzObj)
	COM_INTERFACE_ENTRY(IDispatch)
	COM_INTERFACE_ENTRY(IConnectionPointContainer)
	COM_INTERFACE_ENTRY_IMPL(IConnectionPointContainer)
END_COM_MAP()
BEGIN_CONNECTION_POINT_MAP(CMzObj)
CONNECTION_POINT_ENTRY(DIID__IMzObjEvents)
END_CONNECTION_POINT_MAP()


// IMzObj
public:
 STDMETHOD(Reset)(void);
 STDMETHOD(About)(void);
 STDMETHOD(Eval)(BSTR input,/*[out,retval]*/BSTR *output);
};

#endif //__MZOBJ_H_

