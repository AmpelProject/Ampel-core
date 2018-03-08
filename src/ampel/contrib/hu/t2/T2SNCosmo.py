#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File              : ampel/contrib/hu/examples/t2/T2SNCosmo.py
# Author            : mr <m.rigault@ipnl.in2p3.fr>
# Date              : 06.03.2018
# Last Modified Date: 08.03.2018
# Last Modified By  : mr <m.rigault@ipnl.in2p3.fr>
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod
from ampel.abstract.AbsT2Unit import AbsT2Unit
from ampel.flags.T2RunStates import T2RunStates
import numpy as np

VERSION = 0.1

try:
    import sncosmo
except ImportError:
    raise ImportError("SNCosmo is required for the T2 model T2SNCosmo")

################################
#                              #
#     To Be Made Global        #
#                              #
################################
ZTF_BANDPASSES = {1:{"name":"sdssg"},
                  2:{"name":"sdssr"},
                  3:{"name":"sdssi"},
                 }

def filter_id__to__bandpass_name(filter_id):
    """ """
    if type(filter_id) == int and filter_id in ZTF_BANDPASSES.keys():
        return ZTF_BANDPASSES[filter_id]["name"]
    return filter_id

################################
#                              #
#     Internal Tools           #
#                              #
################################
def mag_to_flux(mag, wavelength, magerr=None):
    """ converts magnitude into flux

    Parameters
    ----------
    mag: [float or array]
        AB magnitude(s)

    wavelength: [float or array]
        central wavelength of the photometric filter.
        In Angstrom

    magerr: [float or array] -optional-
        magnitude error if any

    Returns
    -------
    - float or array (if magerr is None)
    - float or array, float or array (if magerr provided)
    (flux are returned in erg/s/cm2/A)
    """
    flux = 10**(-(mag+2.406)/2.5) / wavelength**2
    if magerr is None:
        return flux
    
    dflux = np.abs(flux*(-magerr/2.5*np.log(10))) # df/f = dcount/count
    return flux,dflux

def get_bandpasses(bandnames):
    """ returns a dictionary with the given bandpasses """
    return {bandname:sncosmo.get_bandpass(bandname) for bandname in np.unique(np.atleast_1d(bandnames))}

################################
#                              #
#  sncosmo<-> Ampel Object     #
#                              #
################################

class SNCosmoTool:
    """ """
    def load_data(self, light_curve):
        """ extract sncosmo data from  Ampel's light_curve 
    
        Returns
        -------
        astropy Table
        """
        from astropy.table import Table
        filter_names = [filter_id__to__bandpass_name(fid) for fid in light_curve.get_values("filter_id")]
        bandpasses   = get_bandpasses(np.unique(filter_names))
        wavelengths  = [bandpasses[bp_].wave_eff for bp_ in filter_names]
        # - fluxes
        flux, fluxerr = np.asarray([mag_to_flux(mag_, lbda_, magerr_) for lbda_, mag_,magerr_ in 
                                    zip(wavelengths, light_curve.get_values("mag"),
                                        light_curve.get_values("magerr")) ]).T
        
        self.sncosmo_data = Table( {"time": light_curve.get_values("obs_date"),
                       "flux": flux,
                       "fluxerr": fluxerr,
                       "band":filter_names,
                       "zp": [25]*len(filter_names),
                       "zpsys":["ab"]*len(filter_names)}
                    )

    def fit_salt2(self, light_curve=None, **kwargs):
        """ **kwargs goes to sncosmo.fit_lc (i.e. bounds)
        """
        if not hasattr(self, "sncosmo_data"):
            if light_curve is None:
                raise ValueError("Provide a lightcurve or first run load_data() method")
            self.load_data()
            
        model = sncosmo.Model(source='salt2')
        if "bounds" not in kwargs or "z" not in kwargs["bounds"]:
            kwargs["bounds"] = {"z":[0,0.2]}
            
        return sncosmo.fit_lc( self.sncosmo_data, model,
                                   model.param_names,  # parameters of model to vary
                                   **kwargs), kwargs   # bounds on parameters (if any)


    def get_results_analysis(self, result, model, **kwargs):
        """ """
        if model in ["salt","Ia","ia","snia", "salt2","SALT2"]:
            return self.get_salt2results_analysis(result, **kwargs)
        
        raise ValueError("Unknown model %s"%model)
    
    def get_salt2results_analysis(self, result, x1_range=[-4,4], c_range=[-1,2] ):
        """ """
        z, t0, x0, x1, c = result["parameters"]
        # - no used yet
        # dz, dt0, dx0, dx1, dc = [result["errors"][k] for k in ['z', 't0', 'x0', 'x1', 'c']]
        
        return {"has_premax_data":  np.any(self.sncosmo_data["time"]<t0),
                "has_postmax_data": np.any(self.sncosmo_data["time"]>t0),
                "x1_in_range": x1>x1_range[0] and x1<x1_range[1],
                "_x1_range":x1_range,
                "c_ok":  c>c_range[0] and c<c_range[1],
                "_c_range": c_range,
                }


    
################################
#                              #
#     T2 Object                #
#                              #
################################
class T2SNCosmo( AbsT2Unit ):
    """
    """
    version = VERSION
    
    def __init__(self, logger, base_config):
        """ """
        self.logger = logger
        if base_config is None:
            self.base_config = {}
            
    # ==================== #
    # AMPEL T2 MANDATORY   #
    # ==================== #    
    def run(self, light_curve, run_parameters):
        """ 

        Parameters
        -----------
        light_curve: "ampel.base.LightCurve" instance. 
             See the LightCurve docstring for more info.

        run_parameters: dict containing run parameters defined in ampel config section:
            T2->runnables->POLYFIT->run_parameters->parameterId
            whereby the parameterId value is defined in the associated t2 document.
            In the case of POLYFIT, parameterId would be either 'default' or 'advanced'.
            A given channel (say CHANNEL_HU_SNIA) would use say the parameterId 'default'
            whereas another channel (say CHANNEL_OKC_SNIIP) would use the parameterId 'advanced'
            
        Returns
        -------
        dict
        """
        try:
            return self._run_(light_curve, run_parameters)
        except:
            return T2RunStates.EXCEPTION


        
    def _run_(self, light_curve, run_parameters):
        """ 
        T2SNCosmo run_parameters:
            - model: [salt2]
            - sncosmo.fit_lc() parameters
        """
        # ------------- #
        #   Input       #
        # ------------- #
        model = run_parameters.pop("model",None)
        # - load the data
        sncosmo_tool = SNCosmoTool()
        sncosmo_tool.load_data(light_curve)

        # ------------- #
        #   LC Fit      #
        # ------------- #
        fit_kwargs = self.base_config if run_parameters is None else {**self.base_config, **run_parameters}
        
        if model in ["salt","Ia","ia","snia", "salt2","SALT2"]:
            if self.logger is not None: self.logger.info("Running T2SNcosmo with *salt2* model")
            #
            [result, fitted_model], kwargs_out = sncosmo_tool.fit_salt2(**fit_kwargs)
            #
        else:
            if self.logger is not None: self.logger.info("Only T2SNCosmo with config model:'Ia' has been implemented. You gave %s"%model)
            raise NotImplementedError("Only T2SNCosmo with config model:'Ia' has been implemented. You gave %s"%model)

        # ------------- #
        #   Output      #
        # ------------- #
        # = Structure the output
        fit_parameters = kwargs_out
        result_param = {}
        for i,pname in enumerate(result.param_names):
            result_param[pname]        = result["parameters"][i]
            result_param[pname+".err"] = result["errors"][pname]

        return {"sncosmo_info": {k: result[k] for k in ["success","chisq","ndof"]},
                "fit_lc_parameters": fit_parameters,
                "model_analysis": sncosmo_tool.get_results_analysis(result, model=model),
                "model":model,
                "fit_acceptable": ~ np.any([ result['ndof']<-1 , result['chisq']/result['ndof']>3 or result['chisq']/result['ndof']<0.2, not result["success"]  ]),
                "fit_results":result_param
                }
