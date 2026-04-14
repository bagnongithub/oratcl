# all.tcl — Oratcl 9.1 ODPI conformance tests (Tcl 9 / tcltest 2.5)
if {"::tcltest" ni [namespace children]} {
    package require tcltest 2.5
    namespace import -force ::tcltest::*
}
set ::tcltest::testsDirectory [file normalize [file dirname [info script]]]

# Default runner config
configure -verbose bps -singleproc 1

set t0 [clock milliseconds]
runAllTests
set elapsed [expr {[clock milliseconds] - $t0}]
puts "\n=== Total wall time: ${elapsed}ms ==="
