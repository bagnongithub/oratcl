//-----------------------------------------------------------------------------
// Copyright (c) 2018, 2024, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpi.c
//   Include this file in your project in order to embed ODPI-C source without
// having to compile files individually. Only the definitions in the file
// include/dpi.h are intended to be used publicly. Each file can also be
// compiled independently if that is preferable.
//-----------------------------------------------------------------------------

#include "dpiConn.c"
#include "dpiContext.c"
#include "dpiData.c"
#include "dpiDebug.c"
#include "dpiDeqOptions.c"
#include "dpiEnqOptions.c"
#include "dpiEnv.c"
#include "dpiError.c"
#include "dpiGen.c"
#include "dpiGlobal.c"
#include "dpiHandleList.c"
#include "dpiHandlePool.c"
#include "dpiJson.c"
#include "dpiLob.c"
#include "dpiMsgProps.c"
#include "dpiObjectAttr.c"
#include "dpiObject.c"
#include "dpiObjectType.c"
#include "dpiOci.c"
#include "dpiOracleType.c"
#include "dpiPool.c"
#include "dpiQueue.c"
#include "dpiRowid.c"
#include "dpiSodaColl.c"
#include "dpiSodaCollCursor.c"
#include "dpiSodaDb.c"
#include "dpiSodaDoc.c"
#include "dpiSodaDocCursor.c"
#include "dpiStmt.c"
#include "dpiStringList.c"
#include "dpiSubscr.c"
#include "dpiUtils.c"
#include "dpiVar.c"
#include "dpiVector.c"
