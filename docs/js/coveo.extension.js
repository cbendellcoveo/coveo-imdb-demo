var CoveoExtension;CoveoExtension =
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ "./src/Index.ts":
/*!**********************!*\
  !*** ./src/Index.ts ***!
  \**********************/
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {


// This entry point defines all the component that should be included in your extension.
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.HelloWorld = void 0;
var HelloWorld_1 = __webpack_require__(/*! ./ui/HelloWorld */ "./src/ui/HelloWorld.ts");
Object.defineProperty(exports, "HelloWorld", ({ enumerable: true, get: function () { return HelloWorld_1.HelloWorld; } }));


/***/ }),

/***/ "./src/ui/HelloWorld.ts":
/*!******************************!*\
  !*** ./src/ui/HelloWorld.ts ***!
  \******************************/
/***/ (function(__unused_webpack_module, exports, __webpack_require__) {


var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.HelloWorld = void 0;
var coveo_search_ui_1 = __webpack_require__(/*! coveo-search-ui */ "coveo-search-ui");
var HelloWorld = /** @class */ (function (_super) {
    __extends(HelloWorld, _super);
    function HelloWorld(element, options, bindings) {
        var _this = _super.call(this, element, HelloWorld.ID, bindings) || this;
        _this.element = element;
        _this.options = options;
        _this.bindings = bindings;
        _this.options = coveo_search_ui_1.ComponentOptions.initComponentOptions(element, HelloWorld, options);
        coveo_search_ui_1.$$(_this.element).text(_this.options.dummyOptionText);
        _this.bind.onRootElement(coveo_search_ui_1.QueryEvents.buildingQuery, function (args) { return _this.handleBuildingQuery(args); });
        return _this;
    }
    HelloWorld.prototype.handleBuildingQuery = function (args) {
        args.queryBuilder.advancedExpression.add(this.options.dummyOptionQuery);
    };
    HelloWorld.ID = 'HelloWorld';
    HelloWorld.options = {
        dummyOptionText: coveo_search_ui_1.ComponentOptions.buildStringOption({
            defaultValue: 'Hello world'
        }),
        dummyOptionQuery: coveo_search_ui_1.ComponentOptions.buildStringOption({
            defaultValue: '@uri'
        })
    };
    return HelloWorld;
}(coveo_search_ui_1.Component));
exports.HelloWorld = HelloWorld;
coveo_search_ui_1.Initialization.registerAutoCreateComponent(HelloWorld);


/***/ }),

/***/ "coveo-search-ui":
/*!************************!*\
  !*** external "Coveo" ***!
  \************************/
/***/ ((module) => {

module.exports = Coveo;

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		if(__webpack_module_cache__[moduleId]) {
/******/ 			return __webpack_module_cache__[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	// module exports must be returned from runtime so entry inlining is disabled
/******/ 	// startup
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__("./src/Index.ts");
/******/ })()
;
//# sourceMappingURL=coveo.extension.js.map