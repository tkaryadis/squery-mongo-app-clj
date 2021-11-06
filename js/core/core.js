var _wispTypes = Object.freeze({
        'list': 'wisp.list',
        'lazy-seq': 'wisp.lazy.seq',
        'set': 'wisp.identity-set'
    });
var isLazySeq = function isLazySeq(value) {
        return value && (_wispTypes || 0)['lazy-seq'] === value.type;
    };
var isIdentitySet = function isIdentitySet(value) {
        return value && (_wispTypes || 0)['set'] === value.type;
    };
var isList = function isList(value) {
        return value && (_wispTypes || 0)['list'] === value.type;
    };
var identity = function identity(x) {
        return x;
    };
var complement = function complement(f) {
        return function () {
            switch (arguments.length) {
            case 0:
                return !f();
            case 1:
                var x = arguments[0];
                return !f(x);
            case 2:
                var x = arguments[0];
                var y = arguments[1];
                return !f(x, y);
            default:
                var x = arguments[0];
                var y = arguments[1];
                var zs = Array.prototype.slice.call(arguments, 2);
                return !f.apply(void 0, [
                    x,
                    y
                ].concat(zs));
            }
        };
    };
var isOdd = function isOdd(n) {
        return n % 2 === 1;
    };
var isEven = function isEven(n) {
        return n % 2 === 0;
    };
var get = function get(target, key, default_) {
        return isSet(target) ? target.has(key) ? key : default_ : 'else' ? target && target.hasOwnProperty(key) ? target[key] : default_ : void 0;
    };
var isDictionary = function isDictionary(form) {
        return isObject(form) && isObject(Object.getPrototypeOf(form)) && isNil(Object.getPrototypeOf(Object.getPrototypeOf(form)));
    };
var dictionary = function dictionary() {
        var pairs = Array.prototype.slice.call(arguments, 0);
        return function loop() {
            var recur = loop;
            var keyValuesø1 = pairs;
            var resultø1 = {};
            do {
                recur = keyValuesø1.length ? (function () {
                    resultø1[keyValuesø1[0]] = keyValuesø1[1];
                    return loop[0] = keyValuesø1.slice(2), loop[1] = resultø1, loop;
                })() : resultø1;
            } while (keyValuesø1 = loop[0], resultø1 = loop[1], recur === loop);
            return recur;
        }.call(this);
    };
var keys = function keys(dictionary) {
        return Object.keys(dictionary);
    };
var vals = function vals(dictionary) {
        return keys(dictionary).map(function (key) {
            return (dictionary || 0)[key];
        });
    };
var keyValues = function keyValues(dictionary) {
        return keys(dictionary).map(function (key) {
            return [
                key,
                (dictionary || 0)[key]
            ];
        });
    };
var merge = function merge() {
        return Object.create(Object.prototype, Array.prototype.slice.call(arguments).reduce(function (descriptor, dictionary) {
            isObject(dictionary) ? Object.keys(dictionary).forEach(function (key) {
                return (descriptor || 0)[key] = Object.getOwnPropertyDescriptor(dictionary, key);
            }) : void 0;
            return descriptor;
        }, Object.create(Object.prototype)));
    };
var isSatisfies = function isSatisfies(protocol, x) {
        return protocol.wisp_core$IProtocol$_ || (x === void 0 ? protocol.wisp_core$IProtocol$nil || false : x === null ? protocol.wisp_core$IProtocol$nil || false : 'else' ? x[protocol.wisp_core$IProtocol$id] || protocol['' + 'wisp_core$IProtocol$' + Object.prototype.toString.call(x).replace('[object ', '').replace(/\]$/, '')] || false : void 0);
    };
var isContainsVector = function isContainsVector(vector, element) {
        return vector.indexOf(element) >= 0;
    };
var mapDictionary = function mapDictionary(source, f) {
        return Object.keys(source).reduce(function (target, key) {
            (target || 0)[key] = f((source || 0)[key]);
            return target;
        }, {});
    };
var toString = Object.prototype.toString;
var isFn = typeof(/./) === 'function' ? function (x) {
                            return toString.call(x) === '[object Function]';
                        } : function (x) {
                            return typeof(x) === 'function';
                        };
var isError = function isError(x) {
        return x instanceof Error || toString.call(x) === '[object Error]';
    };
var isString = function isString(x) {
        return typeof(x) === 'string' || toString.call(x) === '[object String]';
    };
var isNumber = function isNumber(x) {
        return typeof(x) === 'number' || toString.call(x) === '[object Number]';
    };
var isVector = isFn(Array.isArray) ? Array.isArray : function (x) {
        return toString.call(x) === '[object Array]';
    };
var isIterable = function isIterable(x) {
        return isFn((x || 0)[Symbol.iterator]);
    };
var isDate = function isDate(x) {
        return toString.call(x) === '[object Date]';
    };
var isBoolean = function isBoolean(x) {
        return x === true || x === false || toString.call(x) === '[object Boolean]';
    };
var isRePattern = function isRePattern(x) {
        return toString.call(x) === '[object RegExp]';
    };
var isSet = function isSet(x) {
        return x instanceof Set;
    };
var isObject = function isObject(x) {
        return x && typeof(x) === 'object';
    };
var isNil = function isNil(x) {
        return x === void 0 || x === null;
    };
var isTrue = function isTrue(x) {
        return x === true;
    };
var isFalse = function isFalse(x) {
        return x === false;
    };
var reFind = function reFind(re, s) {
        return function () {
            var matchesø1 = re.exec(s);
            return !isNil(matchesø1) ? matchesø1.length === 1 ? (matchesø1 || 0)[0] : matchesø1 : void 0;
        }.call(this);
    };
var reMatches = function reMatches(pattern, source) {
        return function () {
            var matchesø1 = pattern.exec(source);
            return !isNil(matchesø1) && (matchesø1 || 0)[0] === source ? matchesø1.length === 1 ? (matchesø1 || 0)[0] : matchesø1 : void 0;
        }.call(this);
    };
var rePattern = function rePattern(s) {
        return function () {
            var matchø1 = reFind(/^(?:\(\?([idmsux]*)\))?(.*)/, s);
            return new RegExp((matchø1 || 0)[2], (matchø1 || 0)[1]);
        }.call(this);
    };
var inc = function inc(x) {
        return x + 1;
    };
var dec = function dec(x) {
        return x - 1;
    };
var str = function str() {
        return String.prototype.concat.apply('', arguments);
    };
var char = function char(code) {
        return String.fromCharCode(code);
    };
var int = function int(x) {
        return isNumber(x) ? Math.floor(x) : isString(x) ? x.charCodeAt(0) : 'else' ? 0 : void 0;
    };
var subs = function subs(string, start, end) {
        return string.substring(start, end);
    };
var isPatternEqual = function isPatternEqual(x, y) {
    return isRePattern(x) && isRePattern(y) && x.source === y.source && x.global === y.global && x.multiline === y.multiline && x.ignoreCase === y.ignoreCase;
};
var isDateEqual = function isDateEqual(x, y) {
    return isDate(x) && isDate(y) && Number(x) === Number(y);
};
var isSetEqual = function isSetEqual(x, y) {
    return isSet(x) && isSet(y) && x.size === y.size && Array.from(x).every(function ($1) {
        return y.has($1);
    });
};
var isDictionaryEqual = function isDictionaryEqual(x, y) {
    return isObject(x) && isObject(y) && function () {
        var xKeysø1 = keys(x);
        var yKeysø1 = keys(y);
        var xCountø1 = xKeysø1.length;
        var yCountø1 = yKeysø1.length;
        return xCountø1 === yCountø1 && function loop() {
            var recur = loop;
            var indexø1 = 0;
            var countø1 = xCountø1;
            var keysø1 = xKeysø1;
            do {
                recur = indexø1 < countø1 ? isEquivalent((x || 0)[(keysø1 || 0)[indexø1]], (y || 0)[(keysø1 || 0)[indexø1]]) ? (loop[0] = inc(indexø1), loop[1] = countø1, loop[2] = keysø1, loop) : false : true;
            } while (indexø1 = loop[0], countø1 = loop[1], keysø1 = loop[2], recur === loop);
            return recur;
        }.call(this);
    }.call(this);
};
var isEquivalent = function isEquivalent() {
    switch (arguments.length) {
    case 1:
        var x = arguments[0];
        return true;
    case 2:
        var x = arguments[0];
        var y = arguments[1];
        return x === y || (isNil(x) ? isNil(y) : isNil(y) ? isNil(x) : isString(x) ? isString(y) && x.toString() === y.toString() : isNumber(x) ? isNumber(y) && x.valueOf() === y.valueOf() : isSet(x) ? isSetEqual(x, y) : isVector(x) || isList(x) || isLazySeq(x) ? (isVector(y) || isList(y) || isLazySeq(y)) && isEqual._seqEqual(x, y) : isFn(x) ? false : isBoolean(x) ? false : isDate(x) ? isDateEqual(x, y) : isRePattern(x) ? isPatternEqual(x, y) : 'else' ? isDictionaryEqual(x, y) : void 0);
    default:
        var x = arguments[0];
        var y = arguments[1];
        var more = Array.prototype.slice.call(arguments, 2);
        return function loop() {
            var recur = loop;
            var previousø1 = x;
            var currentø1 = y;
            var indexø1 = 0;
            var countø1 = more.length;
            do {
                recur = isEquivalent(previousø1, currentø1) && (indexø1 < countø1 ? (loop[0] = currentø1, loop[1] = (more || 0)[indexø1], loop[2] = inc(indexø1), loop[3] = countø1, loop) : true);
            } while (previousø1 = loop[0], currentø1 = loop[1], indexø1 = loop[2], countø1 = loop[3], recur === loop);
            return recur;
        }.call(this);
    }
};
var isEqual = isEquivalent;
isEqual._wispTypes = _wispTypes;
var notEqual = function notEqual() {
        switch (arguments.length) {
        case 1:
            var x = arguments[0];
            return false;
        case 2:
            var x = arguments[0];
            var y = arguments[1];
            return !isEqual(x, y);
        default:
            var x = arguments[0];
            var y = arguments[1];
            var more = Array.prototype.slice.call(arguments, 2);
            return !isEqual.apply(void 0, [
                x,
                y
            ].concat(more));
        }
    };
var isStrictEqual = function isStrictEqual() {
        switch (arguments.length) {
        case 1:
            var x = arguments[0];
            return true;
        case 2:
            var x = arguments[0];
            var y = arguments[1];
            return x === y;
        default:
            var x = arguments[0];
            var y = arguments[1];
            var more = Array.prototype.slice.call(arguments, 2);
            return function loop() {
                var recur = loop;
                var previousø1 = x;
                var currentø1 = y;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = previousø1 == currentø1 && (indexø1 < countø1 ? (loop[0] = currentø1, loop[1] = (more || 0)[indexø1], loop[2] = inc(indexø1), loop[3] = countø1, loop) : true);
                } while (previousø1 = loop[0], currentø1 = loop[1], indexø1 = loop[2], countø1 = loop[3], recur === loop);
                return recur;
            }.call(this);
        }
    };
var greaterThan = function greaterThan() {
        switch (arguments.length) {
        case 1:
            var x = arguments[0];
            return true;
        case 2:
            var x = arguments[0];
            var y = arguments[1];
            return x > y;
        default:
            var x = arguments[0];
            var y = arguments[1];
            var more = Array.prototype.slice.call(arguments, 2);
            return function loop() {
                var recur = loop;
                var previousø1 = x;
                var currentø1 = y;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = previousø1 > currentø1 && (indexø1 < countø1 ? (loop[0] = currentø1, loop[1] = (more || 0)[indexø1], loop[2] = inc(indexø1), loop[3] = countø1, loop) : true);
                } while (previousø1 = loop[0], currentø1 = loop[1], indexø1 = loop[2], countø1 = loop[3], recur === loop);
                return recur;
            }.call(this);
        }
    };
var notLessThan = function notLessThan() {
        switch (arguments.length) {
        case 1:
            var x = arguments[0];
            return true;
        case 2:
            var x = arguments[0];
            var y = arguments[1];
            return x >= y;
        default:
            var x = arguments[0];
            var y = arguments[1];
            var more = Array.prototype.slice.call(arguments, 2);
            return function loop() {
                var recur = loop;
                var previousø1 = x;
                var currentø1 = y;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = previousø1 >= currentø1 && (indexø1 < countø1 ? (loop[0] = currentø1, loop[1] = (more || 0)[indexø1], loop[2] = inc(indexø1), loop[3] = countø1, loop) : true);
                } while (previousø1 = loop[0], currentø1 = loop[1], indexø1 = loop[2], countø1 = loop[3], recur === loop);
                return recur;
            }.call(this);
        }
    };
var lessThan = function lessThan() {
        switch (arguments.length) {
        case 1:
            var x = arguments[0];
            return true;
        case 2:
            var x = arguments[0];
            var y = arguments[1];
            return x < y;
        default:
            var x = arguments[0];
            var y = arguments[1];
            var more = Array.prototype.slice.call(arguments, 2);
            return function loop() {
                var recur = loop;
                var previousø1 = x;
                var currentø1 = y;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = previousø1 < currentø1 && (indexø1 < countø1 ? (loop[0] = currentø1, loop[1] = (more || 0)[indexø1], loop[2] = inc(indexø1), loop[3] = countø1, loop) : true);
                } while (previousø1 = loop[0], currentø1 = loop[1], indexø1 = loop[2], countø1 = loop[3], recur === loop);
                return recur;
            }.call(this);
        }
    };
var notGreaterThan = function notGreaterThan() {
        switch (arguments.length) {
        case 1:
            var x = arguments[0];
            return true;
        case 2:
            var x = arguments[0];
            var y = arguments[1];
            return x <= y;
        default:
            var x = arguments[0];
            var y = arguments[1];
            var more = Array.prototype.slice.call(arguments, 2);
            return function loop() {
                var recur = loop;
                var previousø1 = x;
                var currentø1 = y;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = previousø1 <= currentø1 && (indexø1 < countø1 ? (loop[0] = currentø1, loop[1] = (more || 0)[indexø1], loop[2] = inc(indexø1), loop[3] = countø1, loop) : true);
                } while (previousø1 = loop[0], currentø1 = loop[1], indexø1 = loop[2], countø1 = loop[3], recur === loop);
                return recur;
            }.call(this);
        }
    };
var sum = function sum() {
        switch (arguments.length) {
        case 0:
            return 0;
        case 1:
            var a = arguments[0];
            return a;
        case 2:
            var a = arguments[0];
            var b = arguments[1];
            return a + b;
        case 3:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            return a + b + c;
        case 4:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            return a + b + c + d;
        case 5:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            return a + b + c + d + e;
        case 6:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            return a + b + c + d + e + f;
        default:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            var more = Array.prototype.slice.call(arguments, 6);
            return function loop() {
                var recur = loop;
                var valueø1 = a + b + c + d + e + f;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = indexø1 < countø1 ? (loop[0] = valueø1 + (more || 0)[indexø1], loop[1] = inc(indexø1), loop[2] = countø1, loop) : valueø1;
                } while (valueø1 = loop[0], indexø1 = loop[1], countø1 = loop[2], recur === loop);
                return recur;
            }.call(this);
        }
    };
var subtract = function subtract() {
        switch (arguments.length) {
        case 0:
            return (function () {
                throw TypeError('Wrong number of args passed to: -');
            })();
        case 1:
            var a = arguments[0];
            return 0 - a;
        case 2:
            var a = arguments[0];
            var b = arguments[1];
            return a - b;
        case 3:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            return a - b - c;
        case 4:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            return a - b - c - d;
        case 5:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            return a - b - c - d - e;
        case 6:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            return a - b - c - d - e - f;
        default:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            var more = Array.prototype.slice.call(arguments, 6);
            return function loop() {
                var recur = loop;
                var valueø1 = a - b - c - d - e - f;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = indexø1 < countø1 ? (loop[0] = valueø1 - (more || 0)[indexø1], loop[1] = inc(indexø1), loop[2] = countø1, loop) : valueø1;
                } while (valueø1 = loop[0], indexø1 = loop[1], countø1 = loop[2], recur === loop);
                return recur;
            }.call(this);
        }
    };
var divide = function divide() {
        switch (arguments.length) {
        case 0:
            return (function () {
                throw TypeError('Wrong number of args passed to: /');
            })();
        case 1:
            var a = arguments[0];
            return 1 / a;
        case 2:
            var a = arguments[0];
            var b = arguments[1];
            return a / b;
        case 3:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            return a / b / c;
        case 4:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            return a / b / c / d;
        case 5:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            return a / b / c / d / e;
        case 6:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            return a / b / c / d / e / f;
        default:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            var more = Array.prototype.slice.call(arguments, 6);
            return function loop() {
                var recur = loop;
                var valueø1 = a / b / c / d / e / f;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = indexø1 < countø1 ? (loop[0] = valueø1 / (more || 0)[indexø1], loop[1] = inc(indexø1), loop[2] = countø1, loop) : valueø1;
                } while (valueø1 = loop[0], indexø1 = loop[1], countø1 = loop[2], recur === loop);
                return recur;
            }.call(this);
        }
    };
var multiply = function multiply() {
        switch (arguments.length) {
        case 0:
            return 1;
        case 1:
            var a = arguments[0];
            return a;
        case 2:
            var a = arguments[0];
            var b = arguments[1];
            return a * b;
        case 3:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            return a * b * c;
        case 4:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            return a * b * c * d;
        case 5:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            return a * b * c * d * e;
        case 6:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            return a * b * c * d * e * f;
        default:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            var more = Array.prototype.slice.call(arguments, 6);
            return function loop() {
                var recur = loop;
                var valueø1 = a * b * c * d * e * f;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = indexø1 < countø1 ? (loop[0] = valueø1 * (more || 0)[indexø1], loop[1] = inc(indexø1), loop[2] = countø1, loop) : valueø1;
                } while (valueø1 = loop[0], indexø1 = loop[1], countø1 = loop[2], recur === loop);
                return recur;
            }.call(this);
        }
    };
var quot = function quot(num, div) {
        return int(num / div);
    };
var mod = function mod(num, div) {
        return num - div * quot(num, div);
    };
var rem_ = function rem_(num, div) {
        return function () {
            var mø1 = mod.apply(void 0, [
                    num,
                    div
                ]);
            return num >= 0 === div >= 0 ? mø1 : mø1 - div;
        }.call(this);
    };
var rem = function () {
        var remø1 = function () {
            return identity(void 0);
        };
        return isNil(1 % 1);
    }.call(this) ? rem_ : function (num, div) {
        return num % div;
    };
var and = function and() {
        switch (arguments.length) {
        case 0:
            return true;
        case 1:
            var a = arguments[0];
            return a;
        case 2:
            var a = arguments[0];
            var b = arguments[1];
            return a && b;
        case 3:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            return a && b && c;
        case 4:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            return a && b && c && d;
        case 5:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            return a && b && c && d && e;
        case 6:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            return a && b && c && d && e && f;
        default:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            var more = Array.prototype.slice.call(arguments, 6);
            return function loop() {
                var recur = loop;
                var valueø1 = a && b && c && d && e && f;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = indexø1 < countø1 ? (loop[0] = valueø1 && (more || 0)[indexø1], loop[1] = inc(indexø1), loop[2] = countø1, loop) : valueø1;
                } while (valueø1 = loop[0], indexø1 = loop[1], countø1 = loop[2], recur === loop);
                return recur;
            }.call(this);
        }
    };
var or = function or() {
        switch (arguments.length) {
        case 0:
            return void 0;
        case 1:
            var a = arguments[0];
            return a;
        case 2:
            var a = arguments[0];
            var b = arguments[1];
            return a || b;
        case 3:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            return a || b || c;
        case 4:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            return a || b || c || d;
        case 5:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            return a || b || c || d || e;
        case 6:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            return a || b || c || d || e || f;
        default:
            var a = arguments[0];
            var b = arguments[1];
            var c = arguments[2];
            var d = arguments[3];
            var e = arguments[4];
            var f = arguments[5];
            var more = Array.prototype.slice.call(arguments, 6);
            return function loop() {
                var recur = loop;
                var valueø1 = a || b || c || d || e || f;
                var indexø1 = 0;
                var countø1 = more.length;
                do {
                    recur = indexø1 < countø1 ? (loop[0] = valueø1 || (more || 0)[indexø1], loop[1] = inc(indexø1), loop[2] = countø1, loop) : valueø1;
                } while (valueø1 = loop[0], indexø1 = loop[1], countø1 = loop[2], recur === loop);
                return recur;
            }.call(this);
        }
    };
var print = function print() {
        var more = Array.prototype.slice.call(arguments, 0);
        return console.log.apply(void 0, more);
    };
var max = Math.max;
var min = Math.min;
var isNan = isNaN;