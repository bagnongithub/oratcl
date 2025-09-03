
Building the Tcl oratcl extension for Windows
=============================================

o. Microsoft MSVC++ build:
--------------------------

To build the extension invoke the following command:

    nmake -f makefile.vc INSTALLDIR=<path-to-installed-tcl>

INSTALLDIR is the path of the Tcl distribution where
tcl.h and other needed Tcl files are installed.
To build against a Tcl source build instead,

    nmake -f makefile.vc TCLDIR=<path-to-tcl-sources>

Please look into the makefile.vc file for more options.

To install the extension, invoke the following command:

    nmake -f makefile.vc install

Note that to successfully load the library, you need to have in PATH the Oracle client
libraries.
