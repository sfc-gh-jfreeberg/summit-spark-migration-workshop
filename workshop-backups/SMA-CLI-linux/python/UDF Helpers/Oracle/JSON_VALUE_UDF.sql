-- <copyright file="JSON_VALUE_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================
-- DESCRIPTION: UDF that reproduces the JSON_VALUE function to extract a single result out of a JSON variable
-- PARAMETERS:
--      JSON_OBJECT: VARIANT the JSON variable from which to extract the values
--      JSON_PATH: STRING the JSON path that indicates where the values are located inside the JSON_OBJECT
-- RETURNS:
--      the single value specified by the JSON_PATH inside the JSON_OBJECT. 
--      If the result is not a single value, returns a default error message or an error message defined in
--      the input parameters.
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JSON_VALUE_UDF(JSON_OBJECT VARIANT, JSON_PATH STRING, RETURNING_TYPE STRING, ON_ERROR_MESSAGE VARIANT, ON_EMPTY_MESSAGE VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
IMMUTABLE
<SnowConvertVersionComment>
AS
$$
/* JSONPath 0.8.5 - XPath for JSON
 *
 * Copyright (c) 2007 Stefan Goessner (goessner.net)
 * Licensed under the MIT (MIT-LICENSE.txt) licence.
 *
 * Proposal of Chris Zyp goes into version 0.9.x
 */
function jsonPath(obj, expr, arg) {

  var P = {
    resultType: arg && arg.resultType || "VALUE",
    result: [],
    
    normalize: function(expr) {
      
      var subx = [];
      expr = expr.replace(/(\?\(([^)]+)\))/g, '[$1]').replace(' to ', ':');

return expr.replace(/[\['](\??\(.*?\))[\]']|\['(.*?)'\]/g,
        function ($0, $1, $2) {
          return "[#" + (subx.push($1 || $2) -1) + "]";
})  /* http://code.google.com/p/jsonpath/issues/detail?id=4 */
        .replace(/'?\.'?|\['?/g, ";")
        .replace(/;;;|;;/g, ";..;")
        .replace(/;$|'?\]|'$/g, "")
        .replace(/#([0-9]+)/g, function ($0 , $1) {
          return subx[$1];
        });
    },
      
    asPath: function(path) {
      var x = path.split(";"), p = "$";
      for (var i = 1, n = x.length; i < n; i++) {
        p += /^[0-9*]+$/.test(x[i]) ? ("[" + x[i] + "]") : ("['" + x[i] + "']");
      }
      return p;
    },

    store: function(p, v) {
      if (p) {
        P.result[P.result.length] = P.resultType == "PATH" ? P.asPath(p) : v;
      }
      return !!p;
    },
    
    trace: function(expr, val, path) {
      if (expr !== "") {
        var x = expr.split(";"), loc = x.shift();
        x = x.join(";");
        if (val && val.hasOwnProperty(loc)) {
           P.trace(x, val[loc], path + ";" + loc);
        }
        else if (loc === "*") {
          P.walk(loc, x, val, path, function(m, l, x, v, p) {
            P.trace(m + ";" + x, v, p);
          });
        }
        else if (loc === "..") {
       
          P.trace(x, val, path);
          P.walk(loc, x, val, path, function(m, l, x, v, p) {
            typeof v[m] === "object" && P.trace("..;" + x, v[m], p + ";" + m);
          });
        }
        else if (/^\(.*?\)$/.test(loc)) {// [(expr)]
          // P.trace(P.eval(loc, val, path.substr(path.lastIndexOf(";") + 1)) + 
                           // ";" + x, val, path);
          P.trace(P.filter(loc, val, path.substr(path.lastIndexOf(";") + 1)) + 
                           ";" + x, val, path);
        }
        else if (/^\?\(.*?\)$/.test(loc)) { // [?(expr)]
          P.walk(loc, x, val, path, function (m, l, x, v, p) {
            // if (P.eval(l.replace(/^\?\((.*?)\)$/, "$1"), 
                         // v instanceof Array ? v[m] : v, m)) {
            if (P.filter(l.replace(/^\?\((.*?)\)$/, "$1"), v instanceof Array ? v[m] : v, m)) {
              var newExpr = m === "" ? m : m + ";";
              newExpr = newExpr + x;
              P.trace(newExpr, v, p);
            }
          }, true); // issue 5 resolved
        }
        else if (/^(-?[0-9]*):(-?[0-9]*):?([0-9]*)$/.test(loc)) { // [start:end:step]  phyton slice syntax
          P.slice(loc, x, val, path);
        }
        else if (/,/.test(loc)) { // [name1,name2,...]
          for (var s = loc.split(/'?,'?/), i = 0, n = s.length; i < n; i++) {
            P.trace(s[i] + ";" + x, val, path);
          }
        }
      }
      else {
        P.store(path, val);
      }
    },
    
    walk: function(loc, expr, val, path, f, toFilter=false) {
      if (val instanceof Array) {
        for (var i = 0, n = val.length; i < n; i++) {
          if (i in val) {
            f(i, loc, expr, val, path);
          }
        }
      }
      // When filtering an object and dont want to return each field in an array
      else if (toFilter) {
        f('', loc, expr, val, path)
      }      
      else if (typeof val === "object") {
        for (var m in val) {
          if (val.hasOwnProperty(m)) {
            f(m, loc, expr, val, path);
          }
        }
      }
    },
      
    slice: function(loc, expr, val, path) {
      if (val instanceof Array) {
        var len = val.length, start = 0, end = len, step = 1;
        loc.replace(/^(-?[0-9]*):(-?[0-9]*):?(-?[0-9]*)$/g, 
                    function ($0, $1, $2, $3) {
                      start = parseInt($1 || start, 10);
                      end = parseInt($2 || end, 10);
                      step = parseInt($3 || step, 10);
                    });
        start = (start < 0) ? Math.max(0, start + len) : Math.min(len, start);
        end   = (end < 0)   ? Math.max(0, end + len)   : Math.min(len, end);
        for (var i = start; i < end; i += step) {
          P.trace(i + ";" + expr, val, path);
        }
      }
    },
    
    // eval: function(x, _v, _vname) { //console.warn(_vname); // _vname is index
    filter: function(x, _v, _vname) {

      // 01 NOV 2014: filter() replaces eval() for CSP
      
      var s = x.replace(/(^|[^\\])@/g, "$1_v").replace(/\\@/g, "@"),
          p = s.replace('_v.', ''),
          test = false;
          
      var op, comp, attr;

      if ($ && _v) {
      
        attr = /\([^\)]+\)$/.exec(p);      
        attr && (attr = attr[0].replace(/[\(\s\)]/g, ''));

        // console.warn(!!attr);

        if (attr && (op = /\-|\+|\*|\/|\%/.exec(attr))) {          
          test = operate(_v, op[0], attr);
        }
        else if (comp = /==|\<=|\>=|\<|\>/.exec(p)) {
          test = compare(_v, comp[0], p);
        }        
        else {
          test = p in _v;
        }
      }
      
      return test;

      // try {
        
        // return $ && _v && eval(s); 
      // }  // issue 7 : resolved ..
      // catch(e) {
        // throw new SyntaxError("jsonPath: " + e.message + ": " + 
                              // x.replace(/(^|[^\\])@/g, "$1_v")
                               // .replace(/\\@/g, "@"));
      // }  // issue 7 : resolved ..
    }
  };
  
  var operate = function(context, op, attr) {
    // \-|\+|\*|\/|\%
    var attrs = attr.split(op), 
        a = context[attrs[0]], 
        b = attrs[1].replace(/\"/g, '');

    switch(op) {
      case '-' : return a - b;
      case '+' : return a + b;
      case '*' : return a * b;
      case '/' : return a / b;
      case '%' : return a % b;
      default : return false;
    }
  };
  
  var compare = function (context, comp, p) {
    // ==|\<=|\>=|\<|\>
    var attrs = p.replace(/\s/g, '').split(comp), 
        a = context[attrs[0]], 
        b = attrs[1].replace(/\"/g, '');

    switch(comp) {
      case '==' : return a == b;
      case '<=' : return a <= b;
      case '>=' : return a >= b;
      case '<' : return a < b;
      case '>' : return a > b;
      default : return false;
    }
  };
  
  var $ = obj;
  if (expr && obj && (P.resultType == "VALUE" || P.resultType == "PATH")) {
    // expr.replace(/(\?\(([^)]+)\))/g, '[$1]' will enclose all filters with [ ]
    P.trace(P.normalize(expr).replace(/^\$;?/, ""), obj, "$");  // issue 6 resolved
    return P.result.length ? P.result : false;
  }
}

function getOnErrorMessage(onErrorMessage, defaultMessage){
    switch (onErrorMessage){
        case "SSC_ERROR_ON_ERROR":
            return defaultMessage;
        case "SSC_NULL_ON_ERROR":
            return null;
        default:
            return typeof onErrorMessage === 'undefined' ? null : onErrorMessage;
    }
}

try {
    var result = jsonPath(JSON_OBJECT, JSON_PATH);
} catch (error) {
    return getOnErrorMessage(ON_ERROR_MESSAGE, error)
}

var correctReturningType = true;
if (result.length == 1 && typeof RETURNING_TYPE !== 'undefined'){
  correctReturningType = typeof result[0] === RETURNING_TYPE;
}

if (!result) {
    var notFound = 'SSC_CUSTOM_ERROR - NO MATCH FOUND';
    switch (ON_EMPTY_MESSAGE){
        case "SSC_ERROR_ON_EMPTY":
            return notFound;
        case "SSC_NULL_ON_EMPTY":
            return null;
        default:
            return typeof ON_EMPTY_MESSAGE === 'undefined' ? (getOnErrorMessage(ON_ERROR_MESSAGE, notFound)) : ON_EMPTY_MESSAGE;
    }
}

if (result.length > 1 || typeof result[0] === 'object'){
    return getOnErrorMessage(ON_ERROR_MESSAGE, 'SSC_CUSTOM_ERROR - NON SCALAR RESULT');
}

if (!correctReturningType){
    return getOnErrorMessage(ON_ERROR_MESSAGE, 'SSC_CUSTOM_ERROR - INCORRECT RETURNING TYPE');
}

result = result[0];
return result;
$$;