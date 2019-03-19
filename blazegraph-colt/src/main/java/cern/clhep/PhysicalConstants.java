/*
Copyright (c) 1999 CERN - European Organization for Nuclear Research.
Permission to use, copy, modify, distribute and sell this software and its documentation for any purpose 
is hereby granted without fee, provided that the above copyright notice appear in all copies and 
that both that copyright notice and this permission notice appear in supporting documentation. 
CERN makes no representations about the suitability of this software for any purpose. 
It is provided "as is" without expressed or implied warranty.
*/
package cern.clhep;

/**
 * High Energy Physics coherent Physical Constants.
 * This class is a Java port of the <a href="http://wwwinfo.cern.ch/asd/lhc++/clhep/manual/RefGuide/Units/PhysicalConstants_h.html">C++ version</a> found in <a href="http://wwwinfo.cern.ch/asd/lhc++/clhep">CLHEP 1.4.0</a>, which in turn has been provided by Geant4 (a simulation toolkit for HEP).
 * <p>
 * For aliasing see {@link #physicalConstants}.
 * 
 * @author wolfgang.hoschek@cern.ch
 * @version 1.0, 09/24/99
 */
public class PhysicalConstants extends Object {
/**
 * Little trick to allow for "aliasing", that is, renaming this class.
 * Normally you would write
 * <pre>
 * PhysicalConstants.twopi;
 * PhysicalConstants.c_light;
 * PhysicalConstants.h_Planck;
 * </pre>
 * Since this class has only static methods, but no instance methods
 * you can also shorten the name "PhysicalConstants" to a name that better suits you, for example "P".
 * <pre>
 * PhysicalConstants P = PhysicalConstants.physicalConstants; // kind of "alias"
 * P.twopi;
 * P.c_light;
 * P.h_Planck;
 * </pre>
 */
public static final PhysicalConstants physicalConstants = new PhysicalConstants();

//
// 
//
public static final double     pi  = Math.PI; //3.14159265358979323846;
public static final double  twopi  = 2*pi;
public static final double halfpi  = pi/2;
public static final double     pi2 = pi*pi;

//
// 
//
public static final double Avogadro = 6.0221367e+23/Units.mole;

//
// c   = 299.792458 mm/ns
// c^2 = 898.7404 (mm/ns)^2 
//
public static final double c_light   = 2.99792458e+8 * Units.m/Units.s;
public static final double c_squared = c_light * c_light;

//
// h     = 4.13566e-12 MeV*ns
// hbar  = 6.58212e-13 MeV*ns
// hbarc = 197.32705e-12 MeV*mm
//
public static final double h_Planck      = 6.6260755e-34 * Units.joule*Units.s;
public static final double hbar_Planck   = h_Planck/twopi;
public static final double hbarc         = hbar_Planck * c_light;
public static final double hbarc_squared = hbarc * hbarc;

//
//
//
public static final double electron_charge = - Units.eplus; // see SystemOfUnits.h
public static final double e_squared = Units.eplus * Units.eplus;

//
// amu_c2 - atomic equivalent mass unit
// amu    - atomic mass unit
//
public static final double electron_mass_c2 = 0.51099906 * Units.MeV;
public static final double   proton_mass_c2 = 938.27231 * Units.MeV;
public static final double  neutron_mass_c2 = 939.56563 * Units.MeV;
public static final double           amu_c2 = 931.49432 * Units.MeV;
public static final double              amu = amu_c2/c_squared;

//
// permeability of free space mu0    = 2.01334e-16 Mev*(ns*eplus)^2/mm
// permittivity of free space epsil0 = 5.52636e+10 eplus^2/(MeV*mm)
//
public static final double mu0      = 4*pi*1.e-7 * Units.henry/Units.m;
public static final double epsilon0 = 1./(c_squared*mu0);

//
// electromagnetic coupling = 1.43996e-12 MeV*mm/(eplus^2)
//
public static final double elm_coupling           = e_squared/(4*pi*epsilon0);
public static final double fine_structure_const   = elm_coupling/hbarc;
public static final double classic_electr_radius  = elm_coupling/electron_mass_c2;
public static final double electron_Compton_length = hbarc/electron_mass_c2;
public static final double Bohr_radius = electron_Compton_length/fine_structure_const;

public static final double alpha_rcl2 = fine_structure_const
								   *classic_electr_radius
								   *classic_electr_radius;

public static final double twopi_mc2_rcl2 = twopi*electron_mass_c2
											 *classic_electr_radius
											 *classic_electr_radius;
//
//
//
public static final double k_Boltzmann = 8.617385e-11 * Units.MeV/Units.kelvin;

//
//
//
public static final double STP_Temperature = 273.15*Units.kelvin;
public static final double STP_Pressure    = 1.*Units.atmosphere;
public static final double kGasThreshold   = 10.*Units.mg/Units.cm3;

//
//
//
public static final double universe_mean_density = 1.e-25*Units.g/Units.cm3;
/**
 * Makes this class non instantiable, but still let's others inherit from it.
 */
protected PhysicalConstants() {}
}
