-- <copyright file="JSON_EXTRACT_UDF.sql" company="Snowflake Inc">
--        Copyright (c) 2019-2025 Snowflake Inc. All rights reserved.
-- </copyright>

-- ======================================================================
-- DESCRIPTION: UDF that reproduces JSONExtract, JSONExtractValue AND JSONExtractLargeValue FUNCTIONS to extract 
--      multiple values out of a JSON variable
-- PARAMETERS:
--      JSON_OBJECT: VARIANT the JSON variable from which to extract the values
--      JSON_PATH: STRING the JSON path that indicates where the values are located inside the JSON_OBJECT
--      SINGLE_VALUE: BOOLEAN if true, it return only one value (necessary for JSONExtractValue AND JSONExtractLargeValue), 
--          otherwise return an array (JSONExtract)
-- RETURNS:
--      the values specified by the JSON_PATH inside the JSON_OBJECT
-- ======================================================================
CREATE OR REPLACE FUNCTION PUBLIC.JSON_EXTRACT_UDF(JSON_OBJECT VARIANT, JSON_PATH STRING, SINGLE_VALUE BOOLEAN)
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
          P.trace(P.filter(loc, val, path.substr(path.lastIndexOf(";") + 1)) + 
                           ";" + x, val, path);
        }
        else if (/^\?\(.*?\)$/.test(loc)) { // [?(expr)]
          P.walk(loc, x, val, path, function (m, l, x, v, p) {
            if (P.filter(l.replace(/^\?\((.*?)\)$/, "$1"), 
                         v instanceof Array ? v[m] : v, m)) {
              P.trace(m + ";" + x, v, p);
            }
          });
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
    
    walk: function(loc, expr, val, path, f) {
      if (val instanceof Array) {
        for (var i = 0, n = val.length; i < n; i++) {
          if (i in val) {
            f(i, loc, expr, val, path);
          }
        }
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
    
    filter: function(x, _v, _vname) {

      // filter() replaces eval() for CSP      
      var s = x.replace(/(^|[^\\])@/g, "$1_v").replace(/\\@/g, "@"),
          p = s.replace('_v.', ''),
          test = false;
          
      var op, comp, attr;

      if ($ && _v) {
      
        attr = /\([^\)]+\)$/.exec(p);      
        attr && (attr = attr[0].replace(/[\(\s\)]/g, ''));

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
    }
  };
  
  var operate = function(context, op, attr) {
    // \-|\+|\*|\/|\%
    var attrs = attr.split(op), 
        a = context[attrs[0]], 
        b = attrs[1];

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
        b = attrs[1];

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
    P.trace(P.normalize(expr).replace(/^\$;?/, ""), obj, "$");
    return P.result.length ? P.result : false;
  }
}
try {
    var result = jsonPath(JSON_OBJECT, JSON_PATH);
} catch (error) {
    var result = null;
}

if (!result) {
  return null;
}

if (SINGLE_VALUE){
    result = result[0];
    if (typeof result === 'object' ){
        return result;
    }
    return String(result);
}

return result;
$$;